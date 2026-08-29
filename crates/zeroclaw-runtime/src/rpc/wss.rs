//! WebSocket Secure (WSS) transport for the RPC layer.
//! Mirrors the Unix socket transport (`unix.rs`) but uses TLS-encrypted
//! WebSocket connections, enabling remote TUI-to-daemon connectivity.

use super::context::RpcContext;
use super::dispatch::RpcDispatcher;
use super::transport::RpcTransport;
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

/// How long the read side waits for any frame before sending a liveness Ping.
const HEARTBEAT_IDLE: Duration = Duration::from_secs(20);

/// How long to wait after a Ping for any frame (a Pong, or anything else)
/// before declaring the peer dead and tearing the connection down.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

/// Best-effort deadline for flushing a WebSocket Close frame during daemon
/// cancellation. A suspended or black-holed peer must not retain the detached
/// writer task and its TLS/socket resources indefinitely.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

/// Backoff after a transient `accept()` error so the serve loop does not
/// hot-spin while the condition (e.g. fd exhaustion) clears.
const ACCEPT_ERROR_BACKOFF_MS: u64 = 50;

/// File-descriptor exhaustion errno values, stable across the Unix targets
/// we support (Linux, macOS, BSD).
#[cfg(unix)]
const EMFILE: i32 = 24; // too many open files (this process)
#[cfg(unix)]
const ENFILE: i32 = 23; // too many open files (system-wide)

fn is_recoverable_accept_error(e: &std::io::Error) -> bool {
    if matches!(
        e.kind(),
        ErrorKind::ConnectionAborted | ErrorKind::Interrupted | ErrorKind::WouldBlock
    ) {
        return true;
    }
    #[cfg(unix)]
    if matches!(e.raw_os_error(), Some(EMFILE) | Some(ENFILE)) {
        return true;
    }
    false
}

// ── Transport ────────────────────────────────────────────────────

/// Control frames the read side asks the writer task to emit out-of-band
/// from the JSON-RPC text stream.
enum Control {
    Ping,
}

async fn run_writer<S>(
    mut sink: S,
    mut writer_rx: mpsc::Receiver<String>,
    mut control_rx: mpsc::Receiver<Control>,
    cancel: CancellationToken,
    close_timeout: Duration,
) where
    S: futures_util::Sink<Message> + Unpin,
{
    loop {
        let msg = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            line = writer_rx.recv() => match line {
                Some(line) => Message::Text(line.into()),
                None => break,
            },
            ctrl = control_rx.recv() => match ctrl {
                Some(Control::Ping) => Message::Ping(Vec::new().into()),
                None => break,
            },
        };
        let sent = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            result = sink.send(msg) => result.is_ok(),
        };
        if !sent {
            break;
        }
    }

    let _ = tokio::time::timeout(close_timeout, sink.close()).await;
}

pub struct WssTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    reader: futures_util::stream::SplitStream<WebSocketStream<S>>,
    writer_tx: mpsc::Sender<String>,
    control_tx: mpsc::Sender<Control>,
    peer_label: String,
    /// Set once a Ping has been sent and we are awaiting any reply. Detects a
    /// peer that went silent on a half-open TCP connection (no FIN/RST).
    awaiting_pong: bool,
}

impl<S> WssTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn new(ws: WebSocketStream<S>, remote_addr: SocketAddr, cancel: CancellationToken) -> Self {
        let peer_label = format!("wss:{remote_addr}");
        let (sink, stream) = ws.split();

        let (writer_tx, writer_rx) = mpsc::channel::<String>(64);
        let (control_tx, control_rx) = mpsc::channel::<Control>(8);
        zeroclaw_spawn::spawn!(async move {
            // Session approval channels and log forwarders retain writer
            // senders past disconnect, so channel closure alone cannot end
            // this task. Cancellation interrupts an in-flight send, then
            // gives the Close-frame flush a bounded best-effort window.
            run_writer(sink, writer_rx, control_rx, cancel, CLOSE_TIMEOUT).await;
        });

        Self {
            reader: stream,
            writer_tx,
            control_tx,
            peer_label,
            awaiting_pong: false,
        }
    }
}

#[async_trait]
impl<S> RpcTransport for WssTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn writer(&self) -> mpsc::Sender<String> {
        self.writer_tx.clone()
    }

    async fn next_frame(&mut self) -> Option<String> {
        loop {
            let idle = if self.awaiting_pong {
                HEARTBEAT_TIMEOUT
            } else {
                HEARTBEAT_IDLE
            };

            match tokio::time::timeout(idle, self.reader.next()).await {
                Err(_) if self.awaiting_pong => return None,
                Err(_) => {
                    if self.control_tx.send(Control::Ping).await.is_err() {
                        return None;
                    }
                    self.awaiting_pong = true;
                }
                Ok(frame) => {
                    self.awaiting_pong = false;
                    match frame {
                        Some(Ok(Message::Text(text))) => return Some(text.to_string()),
                        Some(Ok(Message::Close(_))) | None => return None,
                        Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {
                            continue;
                        }
                        Some(Ok(Message::Binary(_))) => continue,
                        Some(Err(_)) => return None,
                    }
                }
            }
        }
    }

    fn peer_label(&self) -> String {
        self.peer_label.clone()
    }
}

// ── TLS acceptor ─────────────────────────────────────────────────

/// Build a `TlsAcceptor` from PEM-encoded cert and key files.
pub fn build_tls_acceptor(cert_path: &str, key_path: &str) -> Result<TlsAcceptor> {
    use rustls::ServerConfig;
    use rustls_pemfile::{certs, private_key};
    use std::fs::File;
    use std::io::BufReader;

    let cert_file =
        File::open(cert_path).with_context(|| format!("opening TLS cert: {cert_path}"))?;
    let key_file = File::open(key_path).with_context(|| format!("opening TLS key: {key_path}"))?;

    let certs: Vec<_> = certs(&mut BufReader::new(cert_file))
        .collect::<Result<Vec<_>, _>>()
        .context("parsing TLS certificates")?;

    let key = private_key(&mut BufReader::new(key_file))
        .context("parsing TLS private key")?
        .context("no private key found in key file")?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("building TLS server config")?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

// ── Listener ─────────────────────────────────────────────────────

/// Run the WSS RPC listener as a daemon subsystem.
/// `client_count` is incremented on connect, decremented on disconnect —
/// shared with the Unix socket listener for `--ephemeral` shutdown logic.
pub async fn run_wss_listener(
    ctx: Arc<RpcContext>,
    cancel: CancellationToken,
    client_count: Arc<AtomicUsize>,
    tls_acceptor: TlsAcceptor,
    bind_addr: SocketAddr,
) -> Result<()> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .with_context(|| format!("binding WSS listener on {bind_addr}"))?;

    ::zeroclaw_log::record!(
        INFO,
        ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
            .with_attrs(::serde_json::json!({"addr": bind_addr.to_string()})),
        "RPC WSS listener started"
    );

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                ::zeroclaw_log::record!(
                    INFO,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note),
                    "RPC WSS listener shutting down"
                );
                break;
            }
            accept = listener.accept() => {
                let (tcp_stream, remote_addr) = match accept {
                    Ok(v) => v,
                    Err(e) => {
                        if is_recoverable_accept_error(&e) {
                            // Transient (e.g. EMFILE under fd pressure):
                            // the listener is still valid. Back off briefly
                            // to avoid hot-spinning, then keep serving
                            // rather than killing the daemon
                            ::zeroclaw_log::record!(
                                WARN,
                                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                                    .with_outcome(::zeroclaw_log::EventOutcome::Unknown),
                                &format!("WSS accept() transient error: {e}")
                            );
                            tokio::time::sleep(Duration::from_millis(ACCEPT_ERROR_BACKOFF_MS)).await;
                            continue;
                        }
                        return Err(e).context("WSS accept error");
                    }
                };

                let ctx = ctx.clone();
                let count = client_count.clone();
                let acceptor = tls_acceptor.clone();
                let conn_cancel = cancel.child_token();

                count.fetch_add(1, Ordering::Relaxed);

                zeroclaw_spawn::spawn!(async move {
                    // TLS handshake.
                    let tls_stream = match acceptor.accept(tcp_stream).await {
                        Ok(s) => s,
                        Err(e) => {
                            ::zeroclaw_log::record!(
                                WARN,
                                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                                    .with_outcome(::zeroclaw_log::EventOutcome::Unknown),
                                &format!("WSS TLS handshake failed from {remote_addr}: {e}")
                            );
                            count.fetch_sub(1, Ordering::Relaxed);
                            return;
                        }
                    };

                    // WebSocket upgrade.
                    let ws_stream = match tokio_tungstenite::accept_async(tls_stream).await {
                        Ok(ws) => ws,
                        Err(e) => {
                            ::zeroclaw_log::record!(
                                WARN,
                                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                                    .with_outcome(::zeroclaw_log::EventOutcome::Unknown),
                                &format!("WSS WebSocket upgrade failed from {remote_addr}: {e}")
                            );
                            count.fetch_sub(1, Ordering::Relaxed);
                            return;
                        }
                    };

                    let mut transport =
                        WssTransport::new(ws_stream, remote_addr, conn_cancel.clone());
                    let peer = transport.peer_label();
                    let writer_tx = transport.writer();
                    let mut dispatcher = RpcDispatcher::new_with_connection_cancel(
                        ctx.clone(),
                        writer_tx,
                        peer,
                        conn_cancel.clone(),
                    );
                    dispatcher.run_connection(&mut transport).await;

                    if let Some(tui_id) = dispatcher.tui_id() {
                        ctx.tui_registry.unregister(tui_id);
                        use ::zeroclaw_log::Instrument as _;
                        let span = ::zeroclaw_log::info_span!(
                            target: "zeroclaw_log_internal_scope",
                            "zeroclaw_scope",
                            owner_tui_id = %tui_id,
                            channel = "wss",
                        );
                        async {
                            ::zeroclaw_log::record!(
                                INFO,
                                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                                    .with_category(::zeroclaw_log::EventCategory::Agent),
                                "WSS TUI disconnected; sessions retained (persistent)"
                            );
                        }
                        .instrument(span)
                        .await;
                    }

                    count.fetch_sub(1, Ordering::Relaxed);
                });
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod accept_error_tests {
    use super::{Control, WssTransport, is_recoverable_accept_error, run_writer};
    use crate::rpc::dispatch::{Method, RpcDispatcher};
    use crate::rpc::types::InitializeParams;
    use futures_util::{SinkExt, StreamExt};
    use std::io::{Error, ErrorKind};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tokio::sync::mpsc;
    use tokio_tungstenite::WebSocketStream;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::protocol::Role;
    use tokio_util::sync::CancellationToken;
    use zeroclaw_api::jsonrpc::{JSONRPC_VERSION, JsonRpcRequest};

    struct NonReadingPeer {
        write_polled: Arc<AtomicBool>,
    }

    impl AsyncRead for NonReadingPeer {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for NonReadingPeer {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.write_polled.store(true, Ordering::Release);
            Poll::Pending
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn cancellation_bounds_send_and_close_for_non_reading_peer() {
        let write_polled = Arc::new(AtomicBool::new(false));
        let websocket = WebSocketStream::from_raw_socket(
            NonReadingPeer {
                write_polled: Arc::clone(&write_polled),
            },
            Role::Server,
            None,
        )
        .await;
        let (sink, _stream) = websocket.split();
        let (writer_tx, writer_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::channel::<Control>(1);
        let cancel = CancellationToken::new();
        let writer_cancel = cancel.clone();

        let writer = zeroclaw_spawn::spawn!(run_writer(
            sink,
            writer_rx,
            control_rx,
            writer_cancel,
            Duration::from_millis(10),
        ));
        writer_tx.send("response".to_owned()).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while !write_polled.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the WebSocket sink should attempt the blocked network write");

        cancel.cancel();
        tokio::time::timeout(Duration::from_millis(100), writer)
            .await
            .expect("cancellation and bounded Close must release the writer task")
            .expect("writer task should not panic");
    }

    fn rpc_request<T: serde::Serialize>(method: Method, params: &T, id: u64) -> String {
        serde_json::to_string(&JsonRpcRequest::new(
            method.wire_name(),
            serde_json::to_value(params).unwrap(),
            serde_json::Value::Number(id.into()),
        ))
        .unwrap()
    }

    async fn assert_reload_drains_wss_connection_generation() {
        use crate::rpc::dispatch::connection_test_support::{QUEUED_SID, RUNNING_SID, fixture};

        let tmp = tempfile::tempdir().unwrap();
        let fixture = fixture(tmp.path()).await;
        let queued_guard = fixture
            .ctx
            .sessions
            .session_queue
            .acquire(QUEUED_SID)
            .await
            .expect("test should hold the queued session actor");
        let listener_cancel = CancellationToken::new();
        let connection_cancel = listener_cancel.child_token();
        let (server_io, client_io) = tokio::io::duplex(64 * 1024);
        let (server_ws, client_ws) = tokio::join!(
            WebSocketStream::from_raw_socket(server_io, Role::Server, None),
            WebSocketStream::from_raw_socket(client_io, Role::Client, None),
        );
        let mut transport = WssTransport::new(
            server_ws,
            "127.0.0.1:43110".parse().unwrap(),
            connection_cancel.clone(),
        );
        let writer_tx = crate::rpc::transport::RpcTransport::writer(&transport);
        let mut dispatcher = RpcDispatcher::new_with_connection_cancel(
            Arc::clone(&fixture.ctx),
            writer_tx,
            "wss:127.0.0.1:43110".to_string(),
            connection_cancel,
        );
        let connection = zeroclaw_spawn::spawn!(async move {
            dispatcher.run_connection(&mut transport).await;
        });
        let (mut client_sink, mut client_stream) = client_ws.split();

        let init = InitializeParams {
            protocol_version: 1,
            tui_id: None,
            tui_sig: None,
            env: Default::default(),
            client_capabilities: None,
        };
        client_sink
            .send(Message::Text(
                rpc_request(Method::Initialize, &init, 1).into(),
            ))
            .await
            .unwrap();
        let initialized = client_stream
            .next()
            .await
            .expect("initialize response frame")
            .expect("initialize response should be valid");
        let initialized: serde_json::Value = serde_json::from_str(
            initialized
                .to_text()
                .expect("initialize response should be text"),
        )
        .unwrap();
        assert_eq!(initialized["jsonrpc"], JSONRPC_VERSION);
        assert!(initialized["error"].is_null());

        client_sink
            .send(Message::Text(
                rpc_request(
                    Method::SessionPrompt,
                    &serde_json::json!({"session_id": RUNNING_SID, "prompt": "run"}),
                    2,
                )
                .into(),
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), fixture.provider_started.notified())
            .await
            .expect("running prompt should reach the provider");

        client_sink
            .send(Message::Text(
                rpc_request(
                    Method::SessionPrompt,
                    &serde_json::json!({"session_id": QUEUED_SID, "prompt": "queue"}),
                    3,
                )
                .into(),
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while fixture
                .ctx
                .sessions
                .session_queue
                .queue_depth(QUEUED_SID)
                .await
                < 2
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second prompt should be waiting in the session actor queue");

        listener_cancel.cancel();
        tokio::time::timeout(Duration::from_secs(7), connection)
            .await
            .expect("WSS connection must drain generation-owned prompt tasks")
            .expect("WSS connection task should not panic");

        assert!(
            fixture.provider_dropped.load(Ordering::Acquire),
            "reload must cancel and join the running provider future"
        );
        assert_eq!(
            fixture.queued_provider_calls.load(Ordering::Acquire),
            0,
            "a queued prompt from the closed generation must never execute"
        );
        assert_eq!(
            fixture
                .ctx
                .sessions
                .session_queue
                .queue_depth(QUEUED_SID)
                .await,
            1,
            "only the deliberate test holder should remain queued"
        );
        assert!(!fixture.ctx.sessions.has_inflight_turn(RUNNING_SID));
        drop(queued_guard);
    }

    #[test]
    fn reload_drains_running_and_queued_prompts_for_wss_connection_generation() {
        std::thread::Builder::new()
            .name("wss-connection-generation".to_string())
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(assert_reload_drains_wss_connection_generation());
            })
            .unwrap()
            .join()
            .expect("WSS connection generation test thread should not panic");
    }

    #[cfg(unix)]
    #[test]
    fn fd_exhaustion_accept_errors_are_recoverable() {
        // EMFILE/ENFILE must not terminate the daemon.
        assert!(is_recoverable_accept_error(&Error::from_raw_os_error(24))); // EMFILE
        assert!(is_recoverable_accept_error(&Error::from_raw_os_error(23))); // ENFILE
    }

    #[test]
    fn transient_kinds_recover_but_fatal_propagates() {
        assert!(is_recoverable_accept_error(&Error::from(
            ErrorKind::ConnectionAborted
        )));
        assert!(is_recoverable_accept_error(&Error::from(
            ErrorKind::Interrupted
        )));
        // A non-transient error is not swallowed (loop will propagate it).
        assert!(!is_recoverable_accept_error(&Error::from(
            ErrorKind::InvalidInput
        )));
    }
}
