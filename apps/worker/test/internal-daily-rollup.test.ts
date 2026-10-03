import { afterEach, describe, expect, it, vi } from 'vitest';

import type { Env } from '../src/env';
import worker from '../src/index';
import { createFakeD1Database, type FakeD1QueryHandler } from './helpers/fake-d1';

const NOW_MS = new Date('2026-02-18T00:00:00.000Z').valueOf();
const TODAY_START = Math.floor(NOW_MS / 1000 / 86400) * 86400;
const TARGET_DAY_START = TODAY_START - 86400;

function createEnv(handlers: FakeD1QueryHandler[]): Env {
  return {
    DB: createFakeD1Database(handlers),
    ADMIN_TOKEN: 'test-admin-token',
    UPTIMER_SCHEDULED_ROLLUP_VIA_SERVICE: '1',
  } as unknown as Env;
}

function postRollup(env: Env, body: unknown) {
  return worker.fetch(
    new Request('http://internal/api/v1/internal/rollup/daily', {
      method: 'POST',
      headers: {
        Authorization: 'Bearer test-admin-token',
        'Content-Type': 'application/json; charset=utf-8',
      },
      body: JSON.stringify(body),
    }),
    env,
    { waitUntil: vi.fn() } as unknown as ExecutionContext,
  );
}

describe('internal daily rollup route', () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.clearAllMocks();
  });

  it('requires internal bearer auth', async () => {
    const env = createEnv([]);
    const res = await worker.fetch(
      new Request('http://internal/api/v1/internal/rollup/daily', { method: 'POST' }),
      env,
      { waitUntil: vi.fn() } as unknown as ExecutionContext,
    );

    expect(res.status).toBe(403);
  });

  it('is not found when the service flag is disabled', async () => {
    const env = createEnv([]);
    delete env.UPTIMER_SCHEDULED_ROLLUP_VIA_SERVICE;
    const res = await worker.fetch(
      new Request('http://internal/api/v1/internal/rollup/daily', {
        method: 'POST',
        headers: { Authorization: 'Bearer test-admin-token' },
      }),
      env,
      { waitUntil: vi.fn() } as unknown as ExecutionContext,
    );

    expect(res.status).toBe(404);
  });

  it('rejects non-POST methods and invalid bodies', async () => {
    const env = createEnv([]);

    const get = await worker.fetch(
      new Request('http://internal/api/v1/internal/rollup/daily', {
        headers: { Authorization: 'Bearer test-admin-token' },
      }),
      env,
      { waitUntil: vi.fn() } as unknown as ExecutionContext,
    );
    expect(get.status).toBe(405);

    for (const body of [{ day_start_at: -5 }, { monitor_ids: ['x'] }, { day_start_at: 'no' }]) {
      const res = await postRollup(env, body);
      expect(res.status).toBe(400);
    }
  });

  it('rejects current or future days', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(NOW_MS);
    const env = createEnv([]);

    for (const dayStart of [TODAY_START, TODAY_START + 86400]) {
      const res = await postRollup(env, { day_start_at: dayStart });
      expect(res.status).toBe(400);
    }
  });

  it('rolls up a single monitor chunk for the requested day', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(NOW_MS);
    const rollupInserts: unknown[][] = [];
    const env = createEnv([
      {
        match: 'into locks',
        run: () => ({ meta: { changes: 1 } }),
      },
      {
        match: (sql) =>
          sql.includes('select monitor_id from monitor_daily_rollups where day_start_at'),
        all: () => [],
      },
      {
        match: (sql) =>
          sql.includes('select id, interval_sec, created_at') &&
          sql.includes('from monitors') &&
          sql.includes('where created_at < ?1'),
        all: (args) => {
          expect(args).toEqual([TARGET_DAY_START + 86400, 1]);
          return [{ id: 1, interval_sec: 60, created_at: TARGET_DAY_START - 86400 }];
        },
      },
      {
        match: (sql) =>
          sql.includes(
            'coalesce(sum(case when r.day_start_at >= ?2 then r.total_sec else 0 end), 0) as total_sec_30d',
          ),
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
          rollupInserts.push(args);
          return { meta: { changes: 1 } };
        },
      },
      {
        match: 'insert into public_snapshots',
        run: () => ({ meta: { changes: 1 } }),
      },
    ]);

    const res = await postRollup(env, { day_start_at: TARGET_DAY_START, monitor_ids: [1] });

    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toEqual({
      ok: true,
      day_start_at: TARGET_DAY_START,
      processed: 1,
      total: 1,
      checks_read: 0,
      outages_read: 0,
    });
    expect(rollupInserts).toHaveLength(1);
    expect(rollupInserts[0]?.[0]).toBe(1);
    expect(rollupInserts[0]?.[1]).toBe(TARGET_DAY_START);
  });
});
