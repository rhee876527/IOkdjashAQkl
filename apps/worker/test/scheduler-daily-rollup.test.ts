import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../src/scheduler/lock', () => ({
  acquireLease: vi.fn(),
}));

import type { Env } from '../src/env';
import { acquireLease } from '../src/scheduler/lock';
import {
  getRollupCandidateDayStartsForNow,
  runDailyRollup,
  runDailyRollupForDay,
} from '../src/scheduler/daily-rollup';
import { createFakeD1Database, type FakeD1QueryHandler } from './helpers/fake-d1';

function createEnv(handlers: FakeD1QueryHandler[]): Env {
  return { DB: createFakeD1Database(handlers) } as unknown as Env;
}

const ROLLUP_GUARD_MATCH = 'select monitor_id from monitor_daily_rollups where day_start_at';
const MONITORS_MATCH = (sql: string) =>
  sql.includes('select id, interval_sec, created_at') &&
  sql.includes('from monitors') &&
  sql.includes('where created_at < ?1');
const OVERVIEW_MATCH = (sql: string) =>
  sql.includes(
    'coalesce(sum(case when r.day_start_at >= ?2 then r.total_sec else 0 end), 0) as total_sec_30d',
  ) && sql.includes('left join monitor_daily_rollups r');

describe('scheduler/daily-rollup', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-02-18T00:00:00.000Z'));
    vi.mocked(acquireLease).mockResolvedValue(true);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it('reads outages and checks in batched monitor queries before writing daily rollups', async () => {
    const targetDayStart = Date.UTC(2026, 1, 17, 0, 0, 0) / 1000;
    const targetDayEnd = targetDayStart + 86_400;
    const outageQueryArgs: unknown[][] = [];
    const checkQueryArgs: unknown[][] = [];
    const rollupInsertArgs: unknown[][] = [];
    const snapshotInsertArgs: unknown[][] = [];

    const env = createEnv([
      {
        match: (sql) => sql.includes(ROLLUP_GUARD_MATCH),
        all: () => [],
      },
      {
        match: MONITORS_MATCH,
        all: () => [
          { id: 1, interval_sec: 60, created_at: targetDayStart - 86_400 },
          { id: 2, interval_sec: 60, created_at: targetDayStart - 43_200 },
        ],
      },
      {
        match: OVERVIEW_MATCH,
        all: () => [
          {
            monitor_id: 1,
            total_sec_30d: 86_400,
            downtime_sec_30d: 3_600,
            unknown_sec_30d: 0,
            uptime_sec_30d: 82_800,
            total_sec_90d: 86_400,
            downtime_sec_90d: 3_600,
            unknown_sec_90d: 0,
            uptime_sec_90d: 82_800,
          },
          {
            monitor_id: 2,
            total_sec_30d: 43_200,
            downtime_sec_30d: 0,
            unknown_sec_30d: 0,
            uptime_sec_30d: 43_200,
            total_sec_90d: 43_200,
            downtime_sec_90d: 0,
            unknown_sec_90d: 0,
            uptime_sec_90d: 43_200,
          },
        ],
      },
      {
        match: (sql) => sql.includes('from outages') && sql.includes('monitor_id in'),
        all: (args) => {
          outageQueryArgs.push(args);
          return [
            {
              monitor_id: 1,
              started_at: targetDayStart + 3_600,
              ended_at: targetDayStart + 7_200,
            },
          ];
        },
      },
      {
        match: (sql) => sql.includes('from check_results') && sql.includes('monitor_id in'),
        all: (args) => {
          checkQueryArgs.push(args);
          return [
            {
              monitor_id: 1,
              checked_at: targetDayStart + 3_660,
              status: 'up',
              latency_ms: 45,
            },
            {
              monitor_id: 1,
              checked_at: targetDayStart + 3_720,
              status: 'down',
              latency_ms: null,
            },
            {
              monitor_id: 2,
              checked_at: targetDayStart + 7_200,
              status: 'up',
              latency_ms: 30,
            },
          ];
        },
      },
      {
        match: 'insert into monitor_daily_rollups',
        run: (args) => {
          rollupInsertArgs.push(args);
          return { meta: { changes: 1 } };
        },
      },
      {
        match: 'insert into public_snapshots',
        run: (args) => {
          snapshotInsertArgs.push(args);
          return { meta: { changes: 1 } };
        },
      },
    ]);

    const scheduledTime = Date.UTC(2026, 1, 18, 0, 0, 0);
    const result = await runDailyRollupForDay(env, {
      targetDayStart,
      nowSec: Math.floor(scheduledTime / 1000),
      now: Math.floor(scheduledTime / 1000),
    });

    expect(acquireLease).toHaveBeenCalledWith(
      env.DB,
      `analytics:daily-rollup:${targetDayStart}`,
      Math.floor(scheduledTime / 1000),
      600,
    );
    expect(result).toMatchObject({ processed: 2, total: 2, skipped: null });
    expect(outageQueryArgs).toHaveLength(1);
    expect(checkQueryArgs).toHaveLength(1);
    expect(outageQueryArgs[0]?.slice(0, 2)).toEqual([1, 2]);
    expect(checkQueryArgs[0]?.slice(0, 2)).toEqual([1, 2]);
    expect(outageQueryArgs[0]?.at(-2)).toBe(targetDayEnd);
    expect(outageQueryArgs[0]?.at(-1)).toBe(targetDayStart);
    expect(checkQueryArgs[0]?.at(-2)).toBe(targetDayStart - 3_900);
    expect(checkQueryArgs[0]?.at(-1)).toBe(targetDayEnd);
    expect(rollupInsertArgs).toHaveLength(2);
    expect(rollupInsertArgs[0]?.[0]).toBe(1);
    expect(rollupInsertArgs[0]?.[1]).toBe(targetDayStart);
    expect(rollupInsertArgs[1]?.[0]).toBe(2);
    expect(rollupInsertArgs[1]?.[1]).toBe(targetDayStart);
    expect(snapshotInsertArgs).toHaveLength(1);
    expect(snapshotInsertArgs[0]?.[0]).toBe('analytics-overview');
  });

  it('resumes partial days by processing only monitors missing a rollup row', async () => {
    const targetDayStart = Date.UTC(2026, 1, 17, 0, 0, 0) / 1000;
    const outageQueryArgs: unknown[][] = [];
    const checkQueryArgs: unknown[][] = [];
    const rollupInsertArgs: unknown[][] = [];

    const env = createEnv([
      {
        match: (sql) => sql.includes(ROLLUP_GUARD_MATCH),
        all: () => [{ monitor_id: 1 }],
      },
      {
        match: MONITORS_MATCH,
        all: () => [
          { id: 1, interval_sec: 60, created_at: targetDayStart - 86_400 },
          { id: 2, interval_sec: 60, created_at: targetDayStart - 86_400 },
        ],
      },
      {
        match: OVERVIEW_MATCH,
        all: () => [],
      },
      {
        match: (sql) => sql.includes('from outages') && sql.includes('monitor_id in'),
        all: (args) => {
          outageQueryArgs.push(args);
          return [];
        },
      },
      {
        match: (sql) => sql.includes('from check_results') && sql.includes('monitor_id in'),
        all: (args) => {
          checkQueryArgs.push(args);
          return [];
        },
      },
      {
        match: 'insert into monitor_daily_rollups',
        run: (args) => {
          rollupInsertArgs.push(args);
          return { meta: { changes: 1 } };
        },
      },
      {
        match: 'insert into public_snapshots',
        run: () => ({ meta: { changes: 1 } }),
      },
    ]);

    const result = await runDailyRollupForDay(env, {
      targetDayStart,
      nowSec: targetDayStart + 86_400,
      now: targetDayStart + 86_400,
    });

    expect(result).toMatchObject({ processed: 1, total: 2, skipped: null });
    expect(outageQueryArgs).toHaveLength(1);
    expect(outageQueryArgs[0]?.slice(0, 1)).toEqual([2]);
    expect(checkQueryArgs).toHaveLength(1);
    expect(checkQueryArgs[0]?.slice(0, 1)).toEqual([2]);
    expect(rollupInsertArgs).toHaveLength(1);
    expect(rollupInsertArgs[0]?.[0]).toBe(2);
  });

  it('skips days that are already fully rolled up without reading checks', async () => {
    const targetDayStart = Date.UTC(2026, 1, 17, 0, 0, 0) / 1000;
    let outageCalls = 0;
    let checkCalls = 0;
    let insertCalls = 0;

    const env = createEnv([
      {
        match: (sql) => sql.includes(ROLLUP_GUARD_MATCH),
        all: () => [{ monitor_id: 1 }, { monitor_id: 2 }],
      },
      {
        match: MONITORS_MATCH,
        all: () => [
          { id: 1, interval_sec: 60, created_at: targetDayStart - 86_400 },
          { id: 2, interval_sec: 60, created_at: targetDayStart - 86_400 },
        ],
      },
      {
        match: (sql) => sql.includes('from outages') && sql.includes('monitor_id in'),
        all: () => {
          outageCalls += 1;
          return [];
        },
      },
      {
        match: (sql) => sql.includes('from check_results') && sql.includes('monitor_id in'),
        all: () => {
          checkCalls += 1;
          return [];
        },
      },
      {
        match: 'insert into monitor_daily_rollups',
        run: () => {
          insertCalls += 1;
          return { meta: { changes: 1 } };
        },
      },
    ]);

    const result = await runDailyRollupForDay(env, {
      targetDayStart,
      nowSec: targetDayStart + 86_400,
      now: targetDayStart + 86_400,
    });

    expect(result).toMatchObject({ processed: 0, total: 2, skipped: 'complete' });
    expect(outageCalls).toBe(0);
    expect(checkCalls).toBe(0);
    expect(insertCalls).toBe(0);
  });

  it('restricts eligible monitors when a chunk id filter is provided', async () => {
    const targetDayStart = Date.UTC(2026, 1, 17, 0, 0, 0) / 1000;
    const rollupInsertArgs: unknown[][] = [];
    const monitorQuerySql: string[] = [];

    const env = createEnv([
      {
        match: (sql) => sql.includes(ROLLUP_GUARD_MATCH),
        all: () => [],
      },
      {
        match: MONITORS_MATCH,
        all: (args, normalizedSql) => {
          monitorQuerySql.push(normalizedSql);
          expect(args[0]).toBe(targetDayStart + 86_400);
          return [{ id: 2, interval_sec: 60, created_at: targetDayStart - 86_400 }];
        },
      },
      {
        match: OVERVIEW_MATCH,
        all: () => [],
      },
      {
        match: (sql) => sql.includes('from outages') && sql.includes('monitor_id in'),
        all: () => [],
      },
      {
        match: (sql) => sql.includes('from check_results') && sql.includes('monitor_id in'),
        all: () => [],
      },
      {
        match: 'insert into monitor_daily_rollups',
        run: (args) => {
          rollupInsertArgs.push(args);
          return { meta: { changes: 1 } };
        },
      },
      {
        match: 'insert into public_snapshots',
        run: () => ({ meta: { changes: 1 } }),
      },
    ]);

    const result = await runDailyRollupForDay(env, {
      targetDayStart,
      nowSec: targetDayStart + 86_400,
      now: targetDayStart + 86_400,
      monitorIds: [2],
    });

    expect(monitorQuerySql).toHaveLength(1);
    expect(monitorQuerySql[0]).toContain('and id in');
    expect(result).toMatchObject({ processed: 1, total: 1, skipped: null });
    expect(rollupInsertArgs).toHaveLength(1);
    expect(rollupInsertArgs[0]?.[0]).toBe(2);
  });

  it('keeps check-result batch windows aligned to each monitor interval group', async () => {
    const targetDayStart = Date.UTC(2026, 1, 17, 0, 0, 0) / 1000;
    const targetDayEnd = targetDayStart + 86_400;
    const checkQueryArgs: unknown[][] = [];

    const env = createEnv([
      {
        match: (sql) => sql.includes(ROLLUP_GUARD_MATCH),
        all: () => [],
      },
      {
        match: MONITORS_MATCH,
        all: () => [
          { id: 1, interval_sec: 60, created_at: targetDayStart - 86_400 },
          { id: 2, interval_sec: 60, created_at: targetDayStart - 43_200 },
          { id: 3, interval_sec: 3_600, created_at: targetDayStart - 86_400 },
        ],
      },
      {
        match: OVERVIEW_MATCH,
        all: () => [],
      },
      {
        match: (sql) => sql.includes('from outages') && sql.includes('monitor_id in'),
        all: () => [],
      },
      {
        match: (sql) => sql.includes('from check_results') && sql.includes('monitor_id in'),
        all: (args) => {
          checkQueryArgs.push(args);
          return [];
        },
      },
      {
        match: 'insert into monitor_daily_rollups',
        run: () => ({ meta: { changes: 1 } }),
      },
      {
        match: 'insert into public_snapshots',
        run: () => ({ meta: { changes: 1 } }),
      },
    ]);

    await runDailyRollupForDay(env, {
      targetDayStart,
      nowSec: targetDayEnd,
      now: targetDayEnd,
    });

    expect(checkQueryArgs).toHaveLength(1);
    expect(checkQueryArgs[0]).toEqual([1, 2, 3, targetDayStart - 3_900, targetDayEnd]);
  });

  it('chunks monitor batches to stay under D1 variable limits', async () => {
    const targetDayStart = Date.UTC(2026, 1, 17, 0, 0, 0) / 1000;
    let outageCalls = 0;
    let checkCalls = 0;
    let insertCalls = 0;

    const env = createEnv([
      {
        match: (sql) => sql.includes(ROLLUP_GUARD_MATCH),
        all: () => [],
      },
      {
        match: MONITORS_MATCH,
        all: () =>
          Array.from({ length: 91 }, (_, index) => ({
            id: index + 1,
            interval_sec: 60,
            created_at: targetDayStart - 86_400,
          })),
      },
      {
        match: OVERVIEW_MATCH,
        all: () => [],
      },
      {
        match: (sql) => sql.includes('from outages') && sql.includes('monitor_id in'),
        all: () => {
          outageCalls += 1;
          return [];
        },
      },
      {
        match: (sql) => sql.includes('from check_results') && sql.includes('monitor_id in'),
        all: () => {
          checkCalls += 1;
          return [];
        },
      },
      {
        match: 'insert into monitor_daily_rollups',
        run: () => {
          insertCalls += 1;
          return { meta: { changes: 1 } };
        },
      },
      {
        match: 'insert into public_snapshots',
        run: () => ({ meta: { changes: 1 } }),
      },
    ]);

    const result = await runDailyRollupForDay(env, {
      targetDayStart,
      nowSec: targetDayStart + 86_400,
      now: targetDayStart + 86_400,
    });

    expect(outageCalls).toBe(2);
    expect(checkCalls).toBe(2);
    expect(insertCalls).toBe(91);
    expect(result).toMatchObject({ processed: 91, total: 91, skipped: null });
  });

  it('selects the newest missing days first and bounds the backfill per run', () => {
    const scheduledTime = Date.UTC(2026, 0, 20, 0, 0, 0);
    const day = (monthDay: number) => Date.UTC(2026, 0, monthDay, 0, 0, 0) / 1000;
    expect(getRollupCandidateDayStartsForNow(Math.floor(scheduledTime / 1000))).toEqual([
      day(19),
      day(18),
      day(17),
    ]);
  });

  it('backfills newest missing days while leaving older ones for later runs', async () => {
    const scheduledTime = Date.UTC(2026, 0, 20, 0, 0, 0);
    const day = (monthDay: number) => Date.UTC(2026, 0, monthDay, 0, 0, 0) / 1000;
    const inserted = new Set<string>();
    const rollupInsertDays: number[] = [];

    const env = createEnv([
      {
        match: (sql) => sql.includes(ROLLUP_GUARD_MATCH),
        all: (args) => {
          const dayStart = args[0] as number;
          return [...inserted]
            .filter((key) => key.endsWith(`@${dayStart}`))
            .map((key) => ({ monitor_id: Number(key.split('@')[0]) }));
        },
      },
      {
        match: MONITORS_MATCH,
        all: () => [
          { id: 1, interval_sec: 60, created_at: day(10) },
          { id: 2, interval_sec: 60, created_at: day(10) },
        ],
      },
      {
        match: OVERVIEW_MATCH,
        all: () => [],
      },
      {
        match: (sql) => sql.includes('from outages') && sql.includes('monitor_id in'),
        all: () => [],
      },
      {
        match: (sql) => sql.includes('from check_results') && sql.includes('monitor_id in'),
        all: () => [],
      },
      {
        match: 'insert into monitor_daily_rollups',
        run: (args) => {
          inserted.add(`${args[0]}@${args[1]}`);
          rollupInsertDays.push(args[1] as number);
          return { meta: { changes: 1 } };
        },
      },
      {
        match: 'insert into public_snapshots',
        run: () => ({ meta: { changes: 1 } }),
      },
    ]);

    await runDailyRollup(
      env,
      { scheduledTime } as ScheduledController,
      { waitUntil: vi.fn() } as unknown as ExecutionContext,
    );

    // Newest-first, capped at 3 days per run: older gaps (01-13..01-16) wait
    // for later attempts instead of risking the CPU budget in one invocation.
    expect([...inserted].sort()).toEqual(
      [17, 18, 19].flatMap((monthDay) => [`1@${day(monthDay)}`, `2@${day(monthDay)}`]).sort(),
    );
    expect(rollupInsertDays.slice(0, 2)).toEqual([day(19), day(19)]);
    const rollupLeases = vi
      .mocked(acquireLease)
      .mock.calls.map((call) => call[1])
      .filter((name) => typeof name === 'string' && name.startsWith('analytics:daily-rollup:'));
    expect(rollupLeases).toEqual([
      `analytics:daily-rollup:${day(19)}`,
      `analytics:daily-rollup:${day(18)}`,
      `analytics:daily-rollup:${day(17)}`,
    ]);
  });
});
