import { avg, buildLatencyHistogram, percentileFromValues } from '../analytics/latency';
import {
  buildUnknownIntervals,
  mergeIntervals,
  overlapSeconds,
  sumIntervals,
  type Interval,
} from '../analytics/uptime';
import type { Env } from '../env';
import { refreshPublicAnalyticsOverviewSnapshotIfNeeded } from '../public/analytics-overview';
import { acquireLease } from './lock';

type MonitorRow = {
  id: number;
  interval_sec: number;
  created_at: number;
};

type OutageRow = { monitor_id: number; started_at: number; ended_at: number | null };

type CheckRow = {
  monitor_id: number;
  checked_at: number;
  status: string;
  latency_ms: number | null;
};

function toCheckStatus(value: string | null): 'up' | 'down' | 'maintenance' | 'unknown' {
  switch (value) {
    case 'up':
    case 'down':
    case 'maintenance':
    case 'unknown':
      return value;
    default:
      return 'unknown';
  }
}

const LOCK_LEASE_SECONDS = 10 * 60;
const LOCK_PREFIX = 'analytics:daily-rollup:';
const DAILY_ROLLUP_MONITOR_BATCH_SIZE = 90;

// Backfill window: a lost night heals itself on later runs instead of leaving a
// permanent hole. Bounded so one invocation never blows the CPU budget.
export const ROLLUP_BACKFILL_DAYS = 7;
export const ROLLUP_MAX_DAYS_PER_RUN = 3;

/** Candidate day starts (UTC midnight) for a run, oldest-first, bounded per run. */
export function getRollupCandidateDayStartsForNow(nowSec: number): number[] {
  const todayStart = Math.floor(nowSec / 86400) * 86400;
  const days: number[] = [];
  for (let ago = ROLLUP_BACKFILL_DAYS; ago >= 1; ago -= 1) {
    days.push(todayStart - ago * 86400);
  }
  return days.slice(0, ROLLUP_MAX_DAYS_PER_RUN);
}

function chunkMonitorRows(rows: readonly MonitorRow[], size: number): MonitorRow[][] {
  if (rows.length === 0) {
    return [];
  }

  const chunkSize = Math.max(1, Math.floor(size));
  const chunks: MonitorRow[][] = [];
  for (let index = 0; index < rows.length; index += chunkSize) {
    chunks.push(rows.slice(index, index + chunkSize));
  }
  return chunks;
}

function buildPlaceholders(count: number): string {
  return Array.from({ length: count }, (_, index) => `?${index + 1}`).join(', ');
}

function groupRowsByMonitorId<T extends { monitor_id: number }>(rows: readonly T[]): Map<number, T[]> {
  const grouped = new Map<number, T[]>();
  for (const row of rows) {
    const existing = grouped.get(row.monitor_id);
    if (existing) {
      existing.push(row);
      continue;
    }
    grouped.set(row.monitor_id, [row]);
  }
  return grouped;
}

function groupMonitorRowsByNumber(
  rows: readonly MonitorRow[],
  getKey: (row: MonitorRow) => number,
): Map<number, MonitorRow[]> {
  const grouped = new Map<number, MonitorRow[]>();
  for (const row of rows) {
    const key = getKey(row);
    const existing = grouped.get(key);
    if (existing) {
      existing.push(row);
      continue;
    }
    grouped.set(key, [row]);
  }
  return grouped;
}

async function listOutageRowsForMonitorBatch(
  db: D1Database,
  monitorIds: number[],
  rangeEnd: number,
  earliestRangeStart: number,
): Promise<OutageRow[]> {
  if (monitorIds.length === 0) {
    return [];
  }

  const placeholders = buildPlaceholders(monitorIds.length);
  const { results } = await db
    .prepare(
      `
        SELECT monitor_id, started_at, ended_at
        FROM outages
        WHERE monitor_id IN (${placeholders})
          AND started_at < ?${monitorIds.length + 1}
          AND (ended_at IS NULL OR ended_at > ?${monitorIds.length + 2})
        ORDER BY monitor_id, started_at
      `,
    )
    .bind(...monitorIds, rangeEnd, earliestRangeStart)
    .all<OutageRow>();

  return results ?? [];
}

async function listCheckRowsForMonitorBatch(
  db: D1Database,
  monitorIds: number[],
  checksStart: number,
  rangeEnd: number,
): Promise<CheckRow[]> {
  if (monitorIds.length === 0) {
    return [];
  }

  const placeholders = buildPlaceholders(monitorIds.length);
  const { results } = await db
    .prepare(
      `
        SELECT monitor_id, checked_at, status, latency_ms
        FROM check_results
        WHERE monitor_id IN (${placeholders})
          AND checked_at >= ?${monitorIds.length + 1}
          AND checked_at < ?${monitorIds.length + 2}
        ORDER BY monitor_id, checked_at
      `,
    )
    .bind(...monitorIds, checksStart, rangeEnd)
    .all<CheckRow>();

  return results ?? [];
}

const SAMPLE_GRID_SEC = 60 * 5;

function gcd(a: number, b: number): number {
  while (b !== 0) {
    const t = a % b;
    a = b;
    b = t;
  }
  return a;
}

function maxSampledGapSec(intervalSec: number): number {
  const branch = gcd(intervalSec, 60);
  const lcm = (SAMPLE_GRID_SEC * branch) / gcd(SAMPLE_GRID_SEC, branch);
  return lcm + 60 * branch;
}

export type DailyRollupDayResult = {
  dayStartAt: number;
  leaseAcquired: boolean;
  /** Monitors with a rollup row written by this call. */
  processed: number;
  /** Monitors eligible for the day (including already-rolled ones). */
  total: number;
  checksRead: number;
  outagesRead: number;
  skipped: 'lease' | 'complete' | null;
};

async function listRollupEligibleMonitors(
  db: D1Database,
  targetDayEnd: number,
  onlyIds?: readonly number[],
): Promise<MonitorRow[]> {
  const ids = (onlyIds ?? []).filter((id) => Number.isInteger(id) && id > 0);
  const placeholders = ids.map((_, index) => `?${index + 2}`).join(', ');
  const { results } = await db
    .prepare(
      `
      SELECT id, interval_sec, created_at
      FROM monitors
      WHERE created_at < ?1${ids.length > 0 ? ` AND id IN (${placeholders})` : ''}
      ORDER BY id
    `,
    )
    .bind(targetDayEnd, ...ids)
    .all<MonitorRow>();

  return results ?? [];
}

export async function listRollupEligibleMonitorIds(
  db: D1Database,
  targetDayEnd: number,
): Promise<number[]> {
  const monitors = await listRollupEligibleMonitors(db, targetDayEnd);
  return monitors.map((monitor) => monitor.id);
}

async function listExistingRollupMonitorIds(
  db: D1Database,
  targetDayStart: number,
): Promise<Set<number>> {
  const { results } = await db
    .prepare('SELECT monitor_id FROM monitor_daily_rollups WHERE day_start_at = ?1')
    .bind(targetDayStart)
    .all<{ monitor_id: number }>();

  return new Set((results ?? []).map((row) => row.monitor_id));
}

export async function runDailyRollupForDay(
  env: Env,
  opts: {
    targetDayStart: number;
    nowSec: number;
    now: number;
    /** Restrict to these monitors (service chunking). Intersected with the missing set. */
    monitorIds?: readonly number[];
  },
): Promise<DailyRollupDayResult> {
  const { targetDayStart, nowSec, now } = opts;
  const targetDayEnd = targetDayStart + 86400;

  const lockName = `${LOCK_PREFIX}${targetDayStart}`;
  const acquired = await acquireLease(env.DB, lockName, nowSec, LOCK_LEASE_SECONDS);
  if (!acquired) {
    console.log(`daily-rollup: skip day_start_at=${targetDayStart} reason=lease`);
    return {
      dayStartAt: targetDayStart,
      leaseAcquired: false,
      processed: 0,
      total: 0,
      checksRead: 0,
      outagesRead: 0,
      skipped: 'lease',
    };
  }

  const eligible = await listRollupEligibleMonitors(env.DB, targetDayEnd, opts.monitorIds);
  if (eligible.length === 0) {
    return {
      dayStartAt: targetDayStart,
      leaseAcquired: true,
      processed: 0,
      total: 0,
      checksRead: 0,
      outagesRead: 0,
      skipped: 'complete',
    };
  }

  // Resume guard: retries (and later backfill runs) only process monitors that
  // are still missing a row for the day, so a partial write can never strand
  // the remaining monitors permanently.
  const existing = await listExistingRollupMonitorIds(env.DB, targetDayStart);
  const monitors = eligible.filter((monitor) => !existing.has(monitor.id));
  if (monitors.length === 0) {
    console.log(
      `daily-rollup: skip day_start_at=${targetDayStart} reason=complete total=${eligible.length}`,
    );
    return {
      dayStartAt: targetDayStart,
      leaseAcquired: true,
      processed: 0,
      total: eligible.length,
      checksRead: 0,
      outagesRead: 0,
      skipped: 'complete',
    };
  }

  const wallStart = Date.now();
  const statements: D1PreparedStatement[] = [];
  let processed = 0;
  let checksRead = 0;
  let outagesRead = 0;

  for (const monitorBatch of chunkMonitorRows(monitors, DAILY_ROLLUP_MONITOR_BATCH_SIZE)) {
    const rangeStartByMonitorId = new Map<number, number>();
    for (const monitor of monitorBatch) {
      rangeStartByMonitorId.set(monitor.id, Math.max(targetDayStart, monitor.created_at));
    }

    const earliestRangeStart = monitorBatch.reduce(
      (min, monitor) =>
        Math.min(min, rangeStartByMonitorId.get(monitor.id) ?? targetDayEnd),
      targetDayEnd,
    );
    const monitorIds = monitorBatch.map((monitor) => monitor.id);
    const checkRowsByStart = groupMonitorRowsByNumber(
      monitorBatch,
      (monitor) => {
        const unknownDelay = maxSampledGapSec(monitor.interval_sec);
        return (rangeStartByMonitorId.get(monitor.id) ?? targetDayStart) - unknownDelay;
      },
    );
    const [outageRows, checkRowGroups] = await Promise.all([
      listOutageRowsForMonitorBatch(env.DB, monitorIds, targetDayEnd, earliestRangeStart),
      Promise.all(
        Array.from(checkRowsByStart.entries(), ([checksStart, group]) =>
          listCheckRowsForMonitorBatch(
            env.DB,
            group.map((monitor) => monitor.id),
            checksStart,
            targetDayEnd,
          ),
        ),
      ),
    ]);
    const outageRowsByMonitorId = groupRowsByMonitorId(outageRows);
    const flatCheckRows = checkRowGroups.flat();
    const checkRowsByMonitorId = groupRowsByMonitorId(flatCheckRows);
    outagesRead += outageRows.length;
    checksRead += flatCheckRows.length;

    for (const m of monitorBatch) {
      const rangeStart = rangeStartByMonitorId.get(m.id) ?? targetDayStart;
      const rangeEnd = targetDayEnd;
      if (rangeEnd <= rangeStart) continue;

      const total_sec = Math.max(0, rangeEnd - rangeStart);

      const downtimeIntervals: Interval[] = mergeIntervals(
        (outageRowsByMonitorId.get(m.id) ?? [])
          .map((r) => {
            const start = Math.max(r.started_at, rangeStart);
            const end = Math.min(r.ended_at ?? rangeEnd, rangeEnd);
            return { start, end };
          })
          .filter((it) => it.end > it.start),
      );
      const downtime_sec = sumIntervals(downtimeIntervals);

      const checkRowsForMonitor = checkRowsByMonitorId.get(m.id) ?? [];
      const checks = checkRowsForMonitor.map((r) => ({
        checked_at: r.checked_at,
        status: toCheckStatus(r.status),
      }));

      const unknownIntervals = buildUnknownIntervals(
        rangeStart,
        rangeEnd,
        m.interval_sec,
        checks,
        maxSampledGapSec(m.interval_sec),
      );
      const unknown_sec = Math.max(
        0,
        sumIntervals(unknownIntervals) - overlapSeconds(unknownIntervals, downtimeIntervals),
      );

      const unavailable_sec = downtime_sec;
      const uptime_sec = Math.max(0, total_sec - unavailable_sec);

      let checks_up = 0;
      let checks_down = 0;
      let checks_unknown = 0;
      let checks_maintenance = 0;
      const latencies: number[] = [];

      for (const r of checkRowsForMonitor) {
        if (r.checked_at < rangeStart) continue;
        const st = toCheckStatus(r.status);
        if (st === 'up') {
          checks_up++;
          if (typeof r.latency_ms === 'number' && Number.isFinite(r.latency_ms)) {
            latencies.push(r.latency_ms);
          }
        } else if (st === 'down') {
          checks_down++;
        } else if (st === 'maintenance') {
          checks_maintenance++;
        } else {
          checks_unknown++;
        }
      }

      const checks_total = checks_up + checks_down + checks_unknown + checks_maintenance;

      const avg_latency_ms = avg(latencies);
      const p50_latency_ms = percentileFromValues(latencies, 0.5);
      const p95_latency_ms = percentileFromValues(latencies, 0.95);
      const latency_histogram_json = JSON.stringify(buildLatencyHistogram(latencies));

      statements.push(
        env.DB.prepare(
          `
            INSERT INTO monitor_daily_rollups (
              monitor_id,
              day_start_at,
              total_sec,
              downtime_sec,
              unknown_sec,
              uptime_sec,
              checks_total,
              checks_up,
              checks_down,
              checks_unknown,
              checks_maintenance,
              avg_latency_ms,
              p50_latency_ms,
              p95_latency_ms,
              latency_histogram_json,
              created_at,
              updated_at
            )
            VALUES (
              ?1, ?2, ?3, ?4, ?5, ?6,
              ?7, ?8, ?9, ?10, ?11,
              ?12, ?13, ?14, ?15,
              ?16, ?17
            )
            ON CONFLICT(monitor_id, day_start_at) DO UPDATE SET
              total_sec = excluded.total_sec,
              downtime_sec = excluded.downtime_sec,
              unknown_sec = excluded.unknown_sec,
              uptime_sec = excluded.uptime_sec,
              checks_total = excluded.checks_total,
              checks_up = excluded.checks_up,
              checks_down = excluded.checks_down,
              checks_unknown = excluded.checks_unknown,
              checks_maintenance = excluded.checks_maintenance,
              avg_latency_ms = excluded.avg_latency_ms,
              p50_latency_ms = excluded.p50_latency_ms,
              p95_latency_ms = excluded.p95_latency_ms,
              latency_histogram_json = excluded.latency_histogram_json,
              updated_at = excluded.updated_at
          `,
        ).bind(
          m.id,
          targetDayStart,
          total_sec,
          downtime_sec,
          unknown_sec,
          uptime_sec,
          checks_total,
          checks_up,
          checks_down,
          checks_unknown,
          checks_maintenance,
          avg_latency_ms,
          p50_latency_ms,
          p95_latency_ms,
          latency_histogram_json,
          now,
          now,
        ),
      );

      processed++;
    }

    // Flush per chunk so a kill leaves completable partial progress instead of
    // zero rows for the day (the resume guard picks up the remainder next attempt).
    if (statements.length > 0) {
      await env.DB.batch(statements.splice(0, statements.length));
    }
  }

  await refreshPublicAnalyticsOverviewSnapshotIfNeeded({
    db: env.DB,
    now,
    fullDayEndAt: targetDayEnd,
    force: true,
  });

  const wallMs = Date.now() - wallStart;
  console.log(
    `daily-rollup: day_start_at=${targetDayStart} processed=${processed}/${monitors.length} checks_read=${checksRead} outages_read=${outagesRead} wall_ms=${wallMs}`,
  );

  return {
    dayStartAt: targetDayStart,
    leaseAcquired: true,
    processed,
    total: eligible.length,
    checksRead,
    outagesRead,
    skipped: null,
  };
}

export async function runDailyRollup(
  env: Env,
  controller: ScheduledController,
  _ctx: ExecutionContext,
): Promise<void> {
  const nowSec = Math.floor((controller.scheduledTime ?? Date.now()) / 1000);
  const now = Math.floor(Date.now() / 1000);

  for (const targetDayStart of getRollupCandidateDayStartsForNow(nowSec)) {
    try {
      await runDailyRollupForDay(env, { targetDayStart, nowSec, now });
    } catch (err) {
      console.error(`scheduled: daily rollup failed day_start_at=${targetDayStart}`, err);
    }
  }
}
