import { avg, buildLatencyHistogram, percentileFromValues } from '../analytics/latency';
import {
  buildUnknownIntervals,
  mergeIntervals,
  overlapSeconds,
  sumIntervals,
  utcDayStart,
  type Interval,
} from '../analytics/uptime';
import type { Env } from '../env';
import { acquireLease } from './lock';

type MonitorRow = {
  id: number;
  interval_sec: number;
  created_at: number;
};

type OutageRow = { started_at: number; ended_at: number | null };

type CheckRow = { checked_at: number; status: string; latency_ms: number | null };

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

export async function runDailyRollup(
  env: Env,
  controller: ScheduledController,
  _ctx: ExecutionContext,
): Promise<void> {
  const nowSec = Math.floor((controller.scheduledTime ?? Date.now()) / 1000);
  const todayStart = utcDayStart(nowSec);
  const targetDayStart = todayStart - 86400;
  const targetDayEnd = targetDayStart + 86400;

  const lockName = `${LOCK_PREFIX}${targetDayStart}`;
  const acquired = await acquireLease(env.DB, lockName, nowSec, LOCK_LEASE_SECONDS);
  if (!acquired) return;

  const { results: monitorRows } = await env.DB.prepare(
    `
      SELECT id, interval_sec, created_at
      FROM monitors
      WHERE created_at < ?1
      ORDER BY id
    `,
  )
    .bind(targetDayEnd)
    .all<MonitorRow>();

  const monitors = monitorRows ?? [];
  if (monitors.length === 0) {
    return;
  }

  const statements: D1PreparedStatement[] = [];
  let processed = 0;

  for (const m of monitors) {
    const rangeStart = Math.max(targetDayStart, m.created_at);
    const rangeEnd = targetDayEnd;
    if (rangeEnd <= rangeStart) continue;

    const total_sec = Math.max(0, rangeEnd - rangeStart);

    const { results: outageRows } = await env.DB.prepare(
      `
        SELECT started_at, ended_at
        FROM outages
        WHERE monitor_id = ?1
          AND started_at < ?2
          AND (ended_at IS NULL OR ended_at > ?3)
        ORDER BY started_at
      `,
    )
      .bind(m.id, rangeEnd, rangeStart)
      .all<OutageRow>();

    const downtimeIntervals: Interval[] = mergeIntervals(
      (outageRows ?? [])
        .map((r) => {
          const start = Math.max(r.started_at, rangeStart);
          const end = Math.min(r.ended_at ?? rangeEnd, rangeEnd);
          return { start, end };
        })
        .filter((it) => it.end > it.start),
    );
    const downtime_sec = sumIntervals(downtimeIntervals);

    const checksStart = rangeStart - m.interval_sec * 2;
    const { results: checkRows } = await env.DB.prepare(
      `
        SELECT checked_at, status, latency_ms
        FROM check_results
        WHERE monitor_id = ?1
          AND checked_at >= ?2
          AND checked_at < ?3
        ORDER BY checked_at
      `,
    )
      .bind(m.id, checksStart, rangeEnd)
      .all<CheckRow>();

    const checks = (checkRows ?? []).map((r) => ({
      checked_at: r.checked_at,
      status: toCheckStatus(r.status),
    }));

    const unknownIntervals = buildUnknownIntervals(rangeStart, rangeEnd, m.interval_sec, checks);
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

    for (const r of checkRows ?? []) {
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

    const now = Math.floor(Date.now() / 1000);
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

    // Flush in batches to keep memory bounded.
    if (statements.length >= 50) {
      await env.DB.batch(statements.splice(0, statements.length));
    }
  }

  if (statements.length > 0) {
    await env.DB.batch(statements);
  }

  console.log(
    `daily-rollup: processed ${processed}/${monitors.length} monitors for day_start_at=${targetDayStart}`,
  );
}
