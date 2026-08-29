import type { SessionMessageRow } from '../types/api.ts';
import { generateUUID } from './uuid.ts';

const MAX_MESSAGES = 100;
const PREFIX = 'zeroclaw_chat_history_v1:';

export interface PersistedChatBubble {
  id: string;
  role: 'user' | 'agent';
  content: string;
  thinking?: string;
  markdown?: boolean;
  /** Trusted lifecycle marker retained for a locally durable terminal notice. */
  notice?: boolean;
  /** Verbatim locally-composed user input — never gateway-prefixed, so the
   *  bubble skips stripServerTimestamp for it. (Server rows omit this.) */
  local?: boolean;
  toolCall?: { name: string; args?: unknown; output?: string };
  timestamp: string;
}

function storageKey(sessionId: string): string {
  return `${PREFIX}${sessionId}`;
}

export function loadChatHistory(sessionId: string): PersistedChatBubble[] {
  try {
    const raw = localStorage.getItem(storageKey(sessionId));
    if (!raw) return [];
    const parsed = JSON.parse(raw) as { messages?: PersistedChatBubble[] };
    if (!parsed.messages?.length) return [];
    return parsed.messages;
  } catch {
    return [];
  }
}

export function saveChatHistory(sessionId: string, messages: PersistedChatBubble[]): void {
  try {
    const slice = messages.slice(-MAX_MESSAGES);
    localStorage.setItem(storageKey(sessionId), JSON.stringify({ messages: slice }));
  } catch {
    // QuotaExceeded or private mode
  }
}

/** Map server-persisted rows into UI messages, preserving durable ordering when available. */
export function mapServerMessagesToPersisted(rows: SessionMessageRow[]): PersistedChatBubble[] {
  const base = Date.now() - rows.length * 1000;
  const out: PersistedChatBubble[] = [];
  let idx = 0;
  for (const row of rows) {
    if (row.role === 'system') continue;
    const parsedCreatedAt = row.created_at === null ? Number.NaN : Date.parse(row.created_at);
    const ts = Number.isFinite(parsedCreatedAt)
      ? new Date(parsedCreatedAt).toISOString()
      : new Date(base + idx * 1000).toISOString();
    idx += 1;
    if (row.role === 'user') {
      out.push({
        id: generateUUID(),
        role: 'user',
        content: row.content,
        timestamp: ts,
      });
    } else if (row.role === 'assistant') {
      out.push({
        id: generateUUID(),
        role: 'agent',
        content: row.content,
        markdown: true,
        timestamp: ts,
      });
    } else {
      out.push({
        id: generateUUID(),
        role: 'agent',
        content: row.content,
        markdown: false,
        timestamp: ts,
      });
    }
  }
  return out;
}

export function persistedToUiMessages(
  rows: PersistedChatBubble[],
): Array<{
  id: string;
  role: 'user' | 'agent';
  content: string;
  thinking?: string;
  markdown?: boolean;
  notice?: boolean;
  local?: boolean;
  toolCall?: { name: string; args?: unknown; output?: string };
  timestamp: Date;
}> {
  return rows.map((m) => ({
    id: m.id,
    role: m.role,
    content: m.content,
    thinking: m.thinking,
    markdown: m.markdown,
    notice: m.notice,
    local: m.local,
    toolCall: m.toolCall,
    timestamp: new Date(m.timestamp),
  }));
}

export function uiMessagesToPersisted(
  messages: Array<{
    id: string;
    role: 'user' | 'agent';
    content: string;
    thinking?: string;
    markdown?: boolean;
    notice?: boolean;
    local?: boolean;
    ephemeral?: boolean;
    toolCall?: { name: string; args?: unknown; output?: string };
    timestamp: Date;
  }>,
): PersistedChatBubble[] {
  return messages
    // Skip messages flagged `ephemeral: true` (web slash-command output like
    // /help, /model banners, unknown-command notices). They are throwaway UI
    // feedback and must not be re-hydrated as fake assistant replies on reload. #7137
    .filter((m) => !m.ephemeral)
    .map((m) => ({
      id: m.id,
      role: m.role,
      content: m.content,
      thinking: m.thinking,
      markdown: m.markdown,
      notice: m.notice,
      // Preserve the verbatim-user-input flag so reloaded bubbles still skip
      // server-timestamp stripping.
      local: m.local,
      toolCall: m.toolCall,
      timestamp: m.timestamp.toISOString(),
    }));
}

/**
 * Merge only explicitly retained lifecycle notices into an otherwise
 * authoritative server transcript. A `persisted: false` WebSocket frame marks
 * its notice as locally durable; server hydration must not erase it merely
 * because a session backend exists but missed that message.
 *
 * Each local notice is anchored to the preceding local user turn and matched
 * to the corresponding server user occurrence. This keeps a missed notice
 * before later turns and prevents an unrelated identical assistant message
 * from consuming the fallback.
 */
export function mergeServerHistoryWithLocalNotices(
  server: PersistedChatBubble[],
  local: PersistedChatBubble[],
): PersistedChatBubble[] {
  const insertions = new Map<number, PersistedChatBubble[]>();
  const matchedServerNotices = new Set<number>();
  for (let localIndex = 0; localIndex < local.length; localIndex += 1) {
    const message = local[localIndex];
    if (!message) continue;
    if (message.notice !== true) continue;

    let localAnchorIndex = localIndex - 1;
    while (localAnchorIndex >= 0 && local[localAnchorIndex]?.role !== 'user') {
      localAnchorIndex -= 1;
    }
    const localAnchor = local[localAnchorIndex];
    if (!localAnchor) {
      const at = server.length;
      insertions.set(at, [...(insertions.get(at) ?? []), message]);
      continue;
    }

    let anchorOccurrence = 0;
    for (let index = 0; index <= localAnchorIndex; index += 1) {
      const candidate = local[index];
      if (candidate?.role === 'user' && candidate.content === localAnchor.content) {
        anchorOccurrence += 1;
      }
    }

    let seen = 0;
    const serverAnchorIndex = server.findIndex((candidate) => {
      if (candidate.role !== 'user' || candidate.content !== localAnchor.content) return false;
      seen += 1;
      return seen === anchorOccurrence;
    });
    if (serverAnchorIndex < 0) {
      const at = server.length;
      insertions.set(at, [...(insertions.get(at) ?? []), message]);
      continue;
    }

    const nextTurnOffset = server
      .slice(serverAnchorIndex + 1)
      .findIndex((candidate) => candidate.role === 'user');
    const nextTurnIndex =
      nextTurnOffset < 0 ? server.length : serverAnchorIndex + 1 + nextTurnOffset;
    const persistedNoticeIndex = server.findIndex(
      (candidate, index) =>
        index > serverAnchorIndex &&
        index < nextTurnIndex &&
        candidate.role === message.role &&
        candidate.content === message.content &&
        !matchedServerNotices.has(index),
    );
    if (persistedNoticeIndex >= 0) {
      matchedServerNotices.add(persistedNoticeIndex);
      continue;
    }

    insertions.set(nextTurnIndex, [...(insertions.get(nextTurnIndex) ?? []), message]);
  }

  if (insertions.size === 0) return server;
  const merged: PersistedChatBubble[] = [];
  for (let index = 0; index <= server.length; index += 1) {
    merged.push(...(insertions.get(index) ?? []));
    const current = server[index];
    if (current) merged.push(current);
  }
  return merged;
}
