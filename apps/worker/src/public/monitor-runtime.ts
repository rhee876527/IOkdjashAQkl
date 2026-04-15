import { z } from 'zod';

export const MONITOR_RUNTIME_SNAPSHOT_KEY = 'monitor-runtime';
export const MONITOR_RUNTIME_SNAPSHOT_VERSION = 1;
export const MONITOR_RUNTIME_MAX_AGE_SECONDS = 3 * 60;
export const MONITOR_RUNTIME_HEARTBEAT_POINTS = 60;

const READ_RUNTIME_SNAPSHOT_SQL = `
  SELECT generated_at, body_json
  FROM public_snapshots
  WHERE key = ?1
`;
const UPSERT_RUNTIME_SNAPSHOT_SQL = `
  INSERT INTO public_snapshots (key, generated_at, body_json, updated_at)
  VALUES (?1, ?2, ?3, ?4)
  ON CONFLICT(key) DO UPDATE SET
    generated_at = excluded.generated_at,
    body_json = excluded.body_json,
    updated_at = excluded.updated_at
`;

export type MonitorRuntimeStatusCode = 'u' | 'd' | 'm' | 'x';

export type PublicMonitorRuntimeEntry = {
  monitor_id: number;
  interval_sec: number;
  range_start_at: number | null;
  materialized_at: number;
  last_checked_at: number | null;
  last_status_code: MonitorRuntimeStatusCode;
  last_outage_open: boolean;
  total_sec: number;
  downtime_sec: number;
  unknown_sec: number;
  uptime_sec: number;
  heartbeat_gap_sec: string;
  heartbeat_latency_ms: Array<number | null>;
  heartbeat_status_codes: string;
};

export type PublicMonitorRuntimeSnapshot = {
  version: 1;
  generated_at: number;
  day_start_at: number;
  monitors: PublicMonitorRuntimeEntry[];
};

export type MonitorRuntimeHeartbeat = {
  checked_at: number;
  latency_ms: number | null;
  status: 'up' | 'down' | 'maintenance' | 'unknown';
};

export type MonitorRuntimeTotals = {
  total_sec: number;
  downtime_sec: number;
  unknown_sec: number;
  uptime_sec: number;
  uptime_pct: number | null;
};

export type MonitorRuntimeUpdate = {
  monitor_id: number;
  interval_sec: number;
  created_at: number;
  checked_at: number;
  check_status: string | null;
  next_status: string | null;
  latency_ms: number | null;
};

function parseHeartbeatGapSec(value: string): number[] {
  const trimmed = value.trim();
  if (!trimmed) return [];

  const parts = trimmed.split(',');
  const gaps: number[] = [];
  for (const part of parts) {
    const normalized = part.trim().toLowerCase();
    if (!normalized) return [];
    const parsed = Number.parseInt(normalized, 36);
    if (!Number.isInteger(parsed) || parsed < 0) {
      return [];
    }
    gaps.push(parsed);
  }

  return gaps;
}

function encodeHeartbeatGapSec(gaps: number[]): string {
  if (gaps.length === 0) return '';
  return gaps
    .map((gap) => clampNonNegativeInteger(gap).toString(36))
    .join(',');
}

function heartbeatsToGapSec(
  checkedAt: Array<number | null | undefined>,
): string {
  const gaps: number[] = [];
  for (let index = 1; index < checkedAt.length; index += 1) {
    const newer = checkedAt[index - 1];
    const older = checkedAt[index];
    if (typeof newer !== 'number' || !Number.isInteger(newer)) {
      break;
    }
    if (typeof older !== 'number' || !Number.isInteger(older)) {
      break;
    }
    gaps.push(Math.max(0, newer - older));
  }
  return encodeHeartbeatGapSec(gaps);
}

export function runtimeHeartbeatsToGapSec(
  checkedAt: Array<number | null | undefined>,
): string {
  return heartbeatsToGapSec(checkedAt);
}

const runtimeEntrySchema = z
  .object({
    monitor_id: z.number().int().positive(),
    interval_sec: z.number().int().positive(),
    range_start_at: z.number().int().nonnegative().nullable(),
    materialized_at: z.number().int().nonnegative(),
    last_checked_at: z.number().int().nonnegative().nullable(),
    last_status_code: z.enum(['u', 'd', 'm', 'x']),
    last_outage_open: z.boolean(),
    total_sec: z.number().int().nonnegative(),
    downtime_sec: z.number().int().nonnegative(),
    unknown_sec: z.number().int().nonnegative(),
    uptime_sec: z.number().int().nonnegative(),
    heartbeat_gap_sec: z.string(),
    heartbeat_latency_ms: z
      .array(z.number().int().nonnegative().nullable())
      .max(MONITOR_RUNTIME_HEARTBEAT_POINTS),
    heartbeat_status_codes: z.string().max(MONITOR_RUNTIME_HEARTBEAT_POINTS),
  })
  .superRefine((value, ctx) => {
    const count = value.heartbeat_latency_ms.length;
    if (value.heartbeat_latency_ms.length !== count) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'heartbeat latency length mismatch',
      });
    }
    if (value.heartbeat_status_codes.length !== count) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'heartbeat status code length mismatch',
      });
    }
    for (let index = 0; index < value.heartbeat_status_codes.length; index += 1) {
      const code = value.heartbeat_status_codes[index];
      if (code !== 'u' && code !== 'd' && code !== 'm' && code !== 'x') {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: `invalid heartbeat status code at ${index}`,
        });
        break;
      }
    }
    const gaps = parseHeartbeatGapSec(value.heartbeat_gap_sec);
    if (value.heartbeat_gap_sec.trim().length > 0 && gaps.length === 0) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'invalid heartbeat gap encoding',
      });
      return;
    }
    if (gaps.length !== Math.max(0, count - 1)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'heartbeat gap length mismatch',
      });
    }
    if (count > 0 && value.last_checked_at === null) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'missing last_checked_at for heartbeat strip',
      });
    }
  });

const runtimeSnapshotSchema = z.object({
  version: z.literal(MONITOR_RUNTIME_SNAPSHOT_VERSION),
  generated_at: z.number().int().nonnegative(),
  day_start_at: z.number().int().nonnegative(),
  monitors: z.array(runtimeEntrySchema),
});

const readRuntimeSnapshotStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const upsertRuntimeSnapshotStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

function clampNonNegativeInteger(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.floor(value));
}

export function utcDayStart(timestampSec: number): number {
  return Math.floor(timestampSec / 86400) * 86400;
}

export function toRuntimeStatusCode(value: string | null | undefined): MonitorRuntimeStatusCode {
  switch (value) {
    case 'up':
      return 'u';
    case 'down':
      return 'd';
    case 'maintenance':
      return 'm';
    case 'unknown':
    default:
      return 'x';
  }
}

export function fromRuntimeStatusCode(
  value: MonitorRuntimeStatusCode,
): MonitorRuntimeHeartbeat['status'] {
  switch (value) {
    case 'u':
      return 'up';
    case 'd':
      return 'down';
    case 'm':
      return 'maintenance';
    case 'x':
    default:
      return 'unknown';
  }
}

function readRuntimeSnapshotStatement(db: D1Database): D1PreparedStatement {
  const cached = readRuntimeSnapshotStatementByDb.get(db);
  if (cached) return cached;

  const statement = db.prepare(READ_RUNTIME_SNAPSHOT_SQL);
  readRuntimeSnapshotStatementByDb.set(db, statement);
  return statement;
}

function upsertRuntimeSnapshotStatement(
  db: D1Database,
  snapshot: PublicMonitorRuntimeSnapshot,
  now: number,
): D1PreparedStatement {
  const cached = upsertRuntimeSnapshotStatementByDb.get(db);
  const statement = cached ?? db.prepare(UPSERT_RUNTIME_SNAPSHOT_SQL);
  if (!cached) {
    upsertRuntimeSnapshotStatementByDb.set(db, statement);
  }

  return statement.bind(
    MONITOR_RUNTIME_SNAPSHOT_KEY,
    snapshot.generated_at,
    JSON.stringify(snapshot),
    now,
  );
}

async function readStoredMonitorRuntimeSnapshot(
  db: D1Database,
): Promise<{ generatedAt: number; snapshot: PublicMonitorRuntimeSnapshot } | null> {
  try {
    const row = await readRuntimeSnapshotStatement(db).bind(MONITOR_RUNTIME_SNAPSHOT_KEY).first<{
      generated_at: number;
      body_json: string;
    }>();
    if (!row?.body_json) return null;

    const parsedJson = JSON.parse(row.body_json) as unknown;
    const parsed = runtimeSnapshotSchema.safeParse(parsedJson);
    if (!parsed.success) {
      console.warn('monitor runtime: invalid snapshot payload', parsed.error.message);
      return null;
    }

    return {
      generatedAt: row.generated_at,
      snapshot: parsed.data,
    };
  } catch (err) {
    if (
      err instanceof Error &&
      err.message.startsWith('No fake D1 first() handler matched SQL:')
    ) {
      return null;
    }
    console.warn('monitor runtime: read failed', err);
    return null;
  }
}

export async function readPublicMonitorRuntimeSnapshot(
  db: D1Database,
  now: number,
  maxAgeSeconds = MONITOR_RUNTIME_MAX_AGE_SECONDS,
): Promise<PublicMonitorRuntimeSnapshot | null> {
  const stored = await readStoredMonitorRuntimeSnapshot(db);
  if (!stored) return null;

  const age = Math.max(0, now - stored.generatedAt);
  if (age > maxAgeSeconds) {
    return null;
  }

  const dayStart = utcDayStart(now);
  if (stored.snapshot.day_start_at !== dayStart) {
    return null;
  }

  return stored.snapshot;
}

export async function writePublicMonitorRuntimeSnapshot(
  db: D1Database,
  snapshot: PublicMonitorRuntimeSnapshot,
  now: number,
): Promise<void> {
  await upsertRuntimeSnapshotStatement(db, snapshot, now).run();
}

export function snapshotHasMonitorIds(
  snapshot: PublicMonitorRuntimeSnapshot,
  monitorIds: number[],
): boolean {
  if (monitorIds.length === 0) return true;
  const seen = new Set(snapshot.monitors.map((entry) => entry.monitor_id));
  for (const monitorId of monitorIds) {
    if (!seen.has(monitorId)) {
      return false;
    }
  }
  return true;
}

export function toMonitorRuntimeEntryMap(
  snapshot: PublicMonitorRuntimeSnapshot,
): ReadonlyMap<number, PublicMonitorRuntimeEntry> {
  return new Map(snapshot.monitors.map((entry) => [entry.monitor_id, entry]));
}

function computeSegmentTotals(opts: {
  segmentStart: number;
  segmentEnd: number;
  lastCheckedAt: number | null;
  lastStatusCode: MonitorRuntimeStatusCode;
  lastOutageOpen: boolean;
  intervalSec: number;
}): { totalSec: number; downtimeSec: number; unknownSec: number; uptimeSec: number } {
  const segmentStart = clampNonNegativeInteger(opts.segmentStart);
  const segmentEnd = clampNonNegativeInteger(opts.segmentEnd);
  if (segmentEnd <= segmentStart) {
    return {
      totalSec: 0,
      downtimeSec: 0,
      unknownSec: 0,
      uptimeSec: 0,
    };
  }

  const totalSec = segmentEnd - segmentStart;
  if (opts.lastOutageOpen) {
    return {
      totalSec,
      downtimeSec: totalSec,
      unknownSec: 0,
      uptimeSec: 0,
    };
  }

  if (!isFiniteNumber(opts.lastCheckedAt)) {
    return {
      totalSec,
      downtimeSec: 0,
      unknownSec: totalSec,
      uptimeSec: 0,
    };
  }

  if (opts.lastStatusCode === 'x') {
    return {
      totalSec,
      downtimeSec: 0,
      unknownSec: totalSec,
      uptimeSec: 0,
    };
  }

  const validUntil = opts.lastCheckedAt + Math.max(0, opts.intervalSec) * 2;
  const unknownStart = Math.max(segmentStart, validUntil);
  const unknownSec = segmentEnd > unknownStart ? segmentEnd - unknownStart : 0;

  return {
    totalSec,
    downtimeSec: 0,
    unknownSec,
    uptimeSec: totalSec - unknownSec,
  };
}

function cloneRuntimeEntry(entry: PublicMonitorRuntimeEntry): PublicMonitorRuntimeEntry {
  return {
    ...entry,
    heartbeat_latency_ms: [...entry.heartbeat_latency_ms],
  };
}

function createRuntimeEntryForUpdate(
  update: MonitorRuntimeUpdate,
  dayStart: number,
): PublicMonitorRuntimeEntry {
  const createdToday =
    Number.isFinite(update.created_at) && update.created_at >= dayStart && update.created_at <= update.checked_at;

  return {
    monitor_id: update.monitor_id,
    interval_sec: Math.max(1, clampNonNegativeInteger(update.interval_sec)),
    range_start_at: createdToday ? update.checked_at : dayStart,
    materialized_at: update.checked_at,
    last_checked_at: update.checked_at,
    last_status_code: toRuntimeStatusCode(update.check_status),
    last_outage_open: update.next_status === 'down',
    total_sec: 0,
    downtime_sec: 0,
    unknown_sec: 0,
    uptime_sec: 0,
    heartbeat_gap_sec: '',
    heartbeat_latency_ms: [isFiniteNumber(update.latency_ms) ? Math.round(update.latency_ms) : null],
    heartbeat_status_codes: toRuntimeStatusCode(update.check_status),
  };
}

export function applyMonitorRuntimeUpdates(
  snapshot: PublicMonitorRuntimeSnapshot,
  now: number,
  updates: MonitorRuntimeUpdate[],
): PublicMonitorRuntimeSnapshot {
  const dayStart = utcDayStart(now);
  const nextById = new Map<number, PublicMonitorRuntimeEntry>();
  for (const entry of snapshot.monitors) {
    nextById.set(entry.monitor_id, cloneRuntimeEntry(entry));
  }

  for (const update of updates) {
    if (!Number.isInteger(update.monitor_id) || update.monitor_id <= 0) continue;
    if (!Number.isInteger(update.checked_at) || update.checked_at < 0) continue;

    const existing = nextById.get(update.monitor_id);
    if (!existing) {
      nextById.set(update.monitor_id, createRuntimeEntryForUpdate(update, dayStart));
      continue;
    }

    existing.interval_sec = Math.max(1, clampNonNegativeInteger(update.interval_sec));
    const rangeStartAt = existing.range_start_at;
    const segmentStart =
      rangeStartAt === null ? update.checked_at : Math.max(rangeStartAt, existing.materialized_at);
    const segment = computeSegmentTotals({
      segmentStart,
      segmentEnd: update.checked_at,
      lastCheckedAt: existing.last_checked_at,
      lastStatusCode: existing.last_status_code,
      lastOutageOpen: existing.last_outage_open,
      intervalSec: existing.interval_sec,
    });

    existing.total_sec += segment.totalSec;
    existing.downtime_sec += segment.downtimeSec;
    existing.unknown_sec += segment.unknownSec;
    existing.uptime_sec += segment.uptimeSec;

    if (existing.range_start_at === null) {
      const createdToday =
        Number.isFinite(update.created_at) &&
        update.created_at >= dayStart &&
        update.created_at <= update.checked_at;
      existing.range_start_at = createdToday ? update.checked_at : dayStart;
    }
    existing.materialized_at = update.checked_at;
    const previousLastCheckedAt = existing.last_checked_at;
    existing.last_checked_at = update.checked_at;
    existing.last_status_code = toRuntimeStatusCode(update.check_status);
    existing.last_outage_open = update.next_status === 'down';

    existing.heartbeat_latency_ms.unshift(
      isFiniteNumber(update.latency_ms) ? Math.round(update.latency_ms) : null,
    );
    existing.heartbeat_status_codes = `${toRuntimeStatusCode(update.check_status)}${existing.heartbeat_status_codes}`;
    const gaps = parseHeartbeatGapSec(existing.heartbeat_gap_sec);
    if (typeof previousLastCheckedAt === 'number' && Number.isInteger(previousLastCheckedAt)) {
      gaps.unshift(Math.max(0, update.checked_at - previousLastCheckedAt));
    }

    if (existing.heartbeat_latency_ms.length > MONITOR_RUNTIME_HEARTBEAT_POINTS) {
      existing.heartbeat_latency_ms.length = MONITOR_RUNTIME_HEARTBEAT_POINTS;
    }
    if (existing.heartbeat_status_codes.length > MONITOR_RUNTIME_HEARTBEAT_POINTS) {
      existing.heartbeat_status_codes = existing.heartbeat_status_codes.slice(
        0,
        MONITOR_RUNTIME_HEARTBEAT_POINTS,
      );
    }
    if (gaps.length > Math.max(0, existing.heartbeat_latency_ms.length - 1)) {
      gaps.length = Math.max(0, existing.heartbeat_latency_ms.length - 1);
    }
    existing.heartbeat_gap_sec = encodeHeartbeatGapSec(gaps);
  }

  return {
    version: MONITOR_RUNTIME_SNAPSHOT_VERSION,
    generated_at: now,
    day_start_at: dayStart,
    monitors: [...nextById.values()].sort((a, b) => a.monitor_id - b.monitor_id),
  };
}

export function materializeMonitorRuntimeTotals(
  entry: PublicMonitorRuntimeEntry,
  now: number,
): MonitorRuntimeTotals {
  const total_sec = clampNonNegativeInteger(entry.total_sec);
  const downtime_sec = clampNonNegativeInteger(entry.downtime_sec);
  const unknown_sec = clampNonNegativeInteger(entry.unknown_sec);
  const uptime_sec = clampNonNegativeInteger(entry.uptime_sec);

  const segmentStart =
    entry.range_start_at === null
      ? now
      : Math.max(entry.range_start_at, clampNonNegativeInteger(entry.materialized_at));
  const segment = computeSegmentTotals({
    segmentStart,
    segmentEnd: now,
    lastCheckedAt: entry.last_checked_at,
    lastStatusCode: entry.last_status_code,
    lastOutageOpen: entry.last_outage_open,
    intervalSec: entry.interval_sec,
  });

  const totalWithTail = total_sec + segment.totalSec;
  const downtimeWithTail = downtime_sec + segment.downtimeSec;
  const unknownWithTail = unknown_sec + segment.unknownSec;
  const uptimeWithTail = uptime_sec + segment.uptimeSec;

  return {
    total_sec: totalWithTail,
    downtime_sec: downtimeWithTail,
    unknown_sec: unknownWithTail,
    uptime_sec: uptimeWithTail,
    uptime_pct: totalWithTail === 0 ? null : (uptimeWithTail / totalWithTail) * 100,
  };
}

export function runtimeEntryToHeartbeats(
  entry: PublicMonitorRuntimeEntry,
): MonitorRuntimeHeartbeat[] {
  const heartbeats: MonitorRuntimeHeartbeat[] = [];
  const count = Math.min(
    entry.heartbeat_latency_ms.length,
    entry.heartbeat_status_codes.length,
  );
  if (count === 0 || entry.last_checked_at === null) {
    return heartbeats;
  }

  const gaps = parseHeartbeatGapSec(entry.heartbeat_gap_sec);
  let checkedAt = entry.last_checked_at;
  for (let index = 0; index < count; index += 1) {
    const code = entry.heartbeat_status_codes[index] as MonitorRuntimeStatusCode | undefined;
    if (!code) continue;
    heartbeats.push({
      checked_at: checkedAt,
      latency_ms: entry.heartbeat_latency_ms[index] ?? null,
      status: fromRuntimeStatusCode(code),
    });
    const gap = gaps[index];
    if (typeof gap === 'number') {
      checkedAt = Math.max(0, checkedAt - gap);
    }
  }
  return heartbeats;
}

export async function refreshPublicMonitorRuntimeSnapshot(opts: {
  db: D1Database;
  now: number;
  updates: MonitorRuntimeUpdate[];
  rebuild: () => Promise<PublicMonitorRuntimeSnapshot>;
}): Promise<void> {
  const stored = await readStoredMonitorRuntimeSnapshot(opts.db);
  const dayStart = utcDayStart(opts.now);
  const shouldRebuild =
    stored === null ||
    stored.snapshot.day_start_at !== dayStart ||
    stored.generatedAt < dayStart ||
    stored.generatedAt > opts.now;

  if (shouldRebuild) {
    const rebuilt = await opts.rebuild();
    await writePublicMonitorRuntimeSnapshot(opts.db, rebuilt, opts.now);
    return;
  }

  const snapshot = stored.snapshot;
  const missingHistoricalEntry = opts.updates.some(
    (update) =>
      !snapshot.monitors.some((entry) => entry.monitor_id === update.monitor_id) &&
      update.created_at < dayStart &&
      update.checked_at > dayStart + update.interval_sec,
  );
  if (missingHistoricalEntry) {
    const rebuilt = await opts.rebuild();
    await writePublicMonitorRuntimeSnapshot(opts.db, rebuilt, opts.now);
    return;
  }

  const next = applyMonitorRuntimeUpdates(snapshot, opts.now, opts.updates);
  await writePublicMonitorRuntimeSnapshot(opts.db, next, opts.now);
}
