import { parseDbJson, webhookChannelConfigSchema } from '@uptimer/db/json';
import type { MonitorStatus } from '@uptimer/db/schema';

import type { Env } from '../env';
import type { NextState } from '../monitor/state-machine';
import type { CheckOutcome } from '../monitor/types';
import type { WebhookChannel } from '../notify/webhook';

const MAINTENANCE_EVENT_LOOKBACK_SECONDS = 10 * 60;
const D1_MAX_SQL_VARIABLES = 100;
const MAINTENANCE_SUPPRESSED_MONITOR_IDS_BATCH_SIZE = D1_MAX_SQL_VARIABLES - 1;
const LIST_ACTIVE_WEBHOOK_CHANNELS_SQL = `
  SELECT id, name, config_json, created_at
  FROM notification_channels
  WHERE is_active = 1 AND type = 'webhook'
  ORDER BY id
`;
const ACTIVE_WEBHOOK_CHANNELS_CACHE_TTL_MS = 2 * 60_000;

type ActiveWebhookChannelRow = {
  id: number;
  name: string;
  config_json: string;
  created_at: number;
};

type MaintenanceWindowRow = {
  id: number;
  title: string;
  message: string | null;
  starts_at: number;
  ends_at: number;
  created_at: number;
};

type MaintenanceWindowMonitorLinkRow = {
  maintenance_window_id: number;
  monitor_id: number;
};

export type WebhookChannelWithMeta = WebhookChannel & { created_at: number };

export type NotifyContext = {
  ctx: ExecutionContext;
  envRecord: Record<string, unknown>;
  channels: WebhookChannelWithMeta[];
};

export type CompletedNotificationMonitor = {
  row: {
    id: number;
    name: string;
    type: string;
    target: string;
    display_url: string | null;
  };
  checkedAt: number;
  prevStatus: MonitorStatus | null;
  outcome: CheckOutcome;
  next: NextState;
  maintenanceSuppressed: boolean;
};

const listActiveWebhookChannelsStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const activeWebhookChannelsCacheByDb = new WeakMap<
  D1Database,
  { fetchedAtMs: number; channels: WebhookChannelWithMeta[] }
>();

let webhookModulePromise: Promise<typeof import('../notify/webhook')> | null = null;

async function getWebhookDispatchModule() {
  webhookModulePromise ??= import('../notify/webhook');
  return await webhookModulePromise;
}

async function listActiveWebhookChannels(db: D1Database): Promise<WebhookChannelWithMeta[]> {
  const cachedResult = activeWebhookChannelsCacheByDb.get(db);
  if (
    cachedResult &&
    Date.now() - cachedResult.fetchedAtMs < ACTIVE_WEBHOOK_CHANNELS_CACHE_TTL_MS
  ) {
    return cachedResult.channels;
  }

  const cached = listActiveWebhookChannelsStatementByDb.get(db);
  const statement = cached ?? db.prepare(LIST_ACTIVE_WEBHOOK_CHANNELS_SQL);
  if (!cached) {
    listActiveWebhookChannelsStatementByDb.set(db, statement);
  }

  const { results } = await statement.all<ActiveWebhookChannelRow>();

  const channels = (results ?? []).map((r) => ({
    id: r.id,
    name: r.name,
    config: parseDbJson(webhookChannelConfigSchema, r.config_json, { field: 'config_json' }),
    created_at: r.created_at,
  }));

  activeWebhookChannelsCacheByDb.set(db, { fetchedAtMs: Date.now(), channels });
  return channels;
}

export async function createNotifyContext(
  env: Env,
  ctx: ExecutionContext,
): Promise<NotifyContext | null> {
  const channels = await listActiveWebhookChannels(env.DB);
  return channels.length === 0
    ? null
    : { ctx, envRecord: env as unknown as Record<string, unknown>, channels };
}

export async function listMaintenanceSuppressedMonitorIds(
  db: D1Database,
  at: number,
  monitorIds: number[],
): Promise<Set<number>> {
  const ids = [...new Set(monitorIds)];
  if (ids.length === 0) return new Set();

  try {
    const suppressed = new Set<number>();

    for (
      let index = 0;
      index < ids.length;
      index += MAINTENANCE_SUPPRESSED_MONITOR_IDS_BATCH_SIZE
    ) {
      const chunk = ids.slice(index, index + MAINTENANCE_SUPPRESSED_MONITOR_IDS_BATCH_SIZE);
      const placeholders = chunk.map((_, idx) => `?${idx + 2}`).join(', ');
      const sql = `
        SELECT DISTINCT mwm.monitor_id
        FROM maintenance_window_monitors mwm
        JOIN maintenance_windows mw ON mw.id = mwm.maintenance_window_id
        WHERE mw.starts_at <= ?1 AND mw.ends_at > ?1
          AND mwm.monitor_id IN (${placeholders})
      `;

      const { results } = await db
        .prepare(sql)
        .bind(at, ...chunk)
        .all<{ monitor_id: number }>();
      for (const row of results ?? []) {
        suppressed.add(row.monitor_id);
      }
    }

    return suppressed;
  } catch (err) {
    console.warn('notify: failed to list maintenance-suppressed monitor ids', err);
    return new Set();
  }
}

async function listMaintenanceWindowMonitorIdsByWindowId(
  db: D1Database,
  windowIds: number[],
): Promise<Map<number, number[]>> {
  const byWindow = new Map<number, number[]>();
  if (windowIds.length === 0) return byWindow;

  const placeholders = windowIds.map((_, idx) => `?${idx + 1}`).join(', ');
  const sql = `
    SELECT maintenance_window_id, monitor_id
    FROM maintenance_window_monitors
    WHERE maintenance_window_id IN (${placeholders})
    ORDER BY maintenance_window_id, monitor_id
  `;

  const { results } = await db
    .prepare(sql)
    .bind(...windowIds)
    .all<MaintenanceWindowMonitorLinkRow>();
  for (const r of results ?? []) {
    const existing = byWindow.get(r.maintenance_window_id) ?? [];
    existing.push(r.monitor_id);
    byWindow.set(r.maintenance_window_id, existing);
  }

  return byWindow;
}

async function listMaintenanceWindowsStartedBetween(
  db: D1Database,
  startInclusive: number,
  endInclusive: number,
): Promise<MaintenanceWindowRow[]> {
  if (endInclusive < startInclusive) return [];

  const { results } = await db
    .prepare(
      `
      SELECT id, title, message, starts_at, ends_at, created_at
      FROM maintenance_windows
      WHERE starts_at >= ?1 AND starts_at <= ?2
      ORDER BY starts_at ASC, id ASC
    `,
    )
    .bind(startInclusive, endInclusive)
    .all<MaintenanceWindowRow>();

  return results ?? [];
}

async function listMaintenanceWindowsEndedBetween(
  db: D1Database,
  startInclusive: number,
  endInclusive: number,
) {
  if (endInclusive < startInclusive) return [];

  const { results } = await db
    .prepare(
      `
      SELECT id, title, message, starts_at, ends_at, created_at
      FROM maintenance_windows
      WHERE ends_at >= ?1 AND ends_at <= ?2
      ORDER BY ends_at ASC, id ASC
    `,
    )
    .bind(startInclusive, endInclusive)
    .all<MaintenanceWindowRow>();

  return results ?? [];
}

function maintenanceWindowRowToPayload(row: MaintenanceWindowRow, monitorIds: number[]) {
  return {
    id: row.id,
    title: row.title,
    message: row.message,
    starts_at: row.starts_at,
    ends_at: row.ends_at,
    created_at: row.created_at,
    monitor_ids: monitorIds,
  };
}

export async function emitMaintenanceWindowNotifications(
  env: Env,
  notify: NotifyContext,
  now: number,
): Promise<void> {
  const lookbackStart = Math.max(0, now - MAINTENANCE_EVENT_LOOKBACK_SECONDS);

  const [started, ended] = await Promise.all([
    listMaintenanceWindowsStartedBetween(env.DB, lookbackStart, now),
    listMaintenanceWindowsEndedBetween(env.DB, lookbackStart, now),
  ]);

  const windowIds = [...new Set([...started.map((w) => w.id), ...ended.map((w) => w.id)])];
  const monitorIdsByWindowId = await listMaintenanceWindowMonitorIdsByWindowId(env.DB, windowIds);

  for (const w of started) {
    const channelsForEvent = notify.channels.filter((c) => c.created_at <= w.starts_at);
    if (channelsForEvent.length === 0) continue;

    const eventType = 'maintenance.started';
    const eventKey = `maintenance:${w.id}:started:${w.starts_at}`;
    const payload = {
      event: eventType,
      event_id: eventKey,
      timestamp: w.starts_at,
      maintenance: maintenanceWindowRowToPayload(w, monitorIdsByWindowId.get(w.id) ?? []),
    };

    notify.ctx.waitUntil(
      getWebhookDispatchModule()
        .then(({ dispatchWebhookToChannels }) =>
          dispatchWebhookToChannels({
            db: env.DB,
            env: notify.envRecord,
            channels: channelsForEvent,
            eventType,
            eventKey,
            payload,
          }),
        )
        .catch((err) => {
          console.error('notify: failed to dispatch maintenance.started', err);
        }),
    );
  }

  for (const w of ended) {
    const channelsForEvent = notify.channels.filter((c) => c.created_at <= w.ends_at);
    if (channelsForEvent.length === 0) continue;

    const eventType = 'maintenance.ended';
    const eventKey = `maintenance:${w.id}:ended:${w.ends_at}`;
    const payload = {
      event: eventType,
      event_id: eventKey,
      timestamp: w.ends_at,
      maintenance: maintenanceWindowRowToPayload(w, monitorIdsByWindowId.get(w.id) ?? []),
    };

    notify.ctx.waitUntil(
      getWebhookDispatchModule()
        .then(({ dispatchWebhookToChannels }) =>
          dispatchWebhookToChannels({
            db: env.DB,
            env: notify.envRecord,
            channels: channelsForEvent,
            eventType,
            eventKey,
            payload,
          }),
        )
        .catch((err) => {
          console.error('notify: failed to dispatch maintenance.ended', err);
        }),
    );
  }
}

export function queueMonitorNotification(
  env: Env,
  notify: NotifyContext | null,
  completed: CompletedNotificationMonitor,
): void {
  if (!notify || completed.maintenanceSuppressed || !completed.next.changed) {
    return;
  }

  const { row, checkedAt, prevStatus, outcome, next } = completed;

  const prevForEvent: MonitorStatus = prevStatus ?? 'unknown';
  let eventType: 'monitor.down' | 'monitor.up' | null = null;

  if ((prevForEvent === 'up' || prevForEvent === 'unknown') && next.status === 'down') {
    eventType = 'monitor.down';
  } else if (prevForEvent === 'down' && next.status === 'up') {
    eventType = 'monitor.up';
  }

  if (!eventType) {
    return;
  }

  const eventSuffix = eventType === 'monitor.down' ? 'down' : 'up';
  const eventKey = `monitor:${row.id}:${eventSuffix}:${checkedAt}`;

  const payload = {
    event: eventType,
    event_id: eventKey,
    timestamp: checkedAt,
    monitor: {
      id: row.id,
      name: row.name,
      type: row.type,
      target: row.target,
      display_url: row.display_url ?? null,
    },
    state: {
      status: next.status,
      latency_ms: outcome.latencyMs,
      http_status: outcome.httpStatus,
      error: outcome.error,
      location: null,
    },
  };

  notify.ctx.waitUntil(
    getWebhookDispatchModule()
      .then(({ dispatchWebhookToChannels }) =>
        dispatchWebhookToChannels({
          db: env.DB,
          env: notify.envRecord,
          channels: notify.channels,
          eventType,
          eventKey,
          payload,
        }),
      )
      .catch((err) => {
        console.error('notify: failed to dispatch webhooks', err);
      }),
  );
}
