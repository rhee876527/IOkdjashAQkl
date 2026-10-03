import { afterEach, describe, expect, it, vi } from 'vitest';

import type { Env } from '../src/env';
import { runDailyRollupViaService } from '../src/scheduler/scheduled';
import { createFakeD1Database } from './helpers/fake-d1';

function createEnv(selfFetch: (request: Request) => Promise<Response>): Env {
  const day = (monthDay: number) => Date.UTC(2026, 0, monthDay, 0, 0, 0) / 1000;
  const env = {
    DB: createFakeD1Database([
      {
        match: (sql) =>
          sql.includes('select id, interval_sec, created_at') &&
          sql.includes('from monitors') &&
          sql.includes('where created_at < ?1'),
        all: () =>
          Array.from({ length: 7 }, (_, index) => ({
            id: index + 1,
            interval_sec: 60,
            created_at: day(10),
          })),
      },
    ]),
    ADMIN_TOKEN: 'test-admin-token',
    UPTIMER_SCHEDULED_ROLLUP_VIA_SERVICE: '1',
    SELF: { fetch: vi.fn(selfFetch) },
  } as unknown as Env;
  return env;
}

describe('scheduler/daily-rollup service dispatch', () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.clearAllMocks();
  });

  it('fans eligible monitors out in fixed chunks with oldest days first', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const bodies: unknown[] = [];
    const env = createEnv(async (request: Request) => {
      bodies.push(await request.json());
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { 'Content-Type': 'application/json; charset=utf-8' },
      });
    });

    await runDailyRollupViaService(
      env,
      { scheduledTime: Date.UTC(2026, 0, 18, 0, 0, 0) } as ScheduledController,
    );

    const day = (monthDay: number) => Date.UTC(2026, 0, monthDay, 0, 0, 0) / 1000;
    // 3 candidate days x chunks of 5+2, oldest-first, sequential.
    expect(bodies).toEqual([
      { day_start_at: day(11), monitor_ids: [1, 2, 3, 4, 5] },
      { day_start_at: day(11), monitor_ids: [6, 7] },
      { day_start_at: day(12), monitor_ids: [1, 2, 3, 4, 5] },
      { day_start_at: day(12), monitor_ids: [6, 7] },
      { day_start_at: day(13), monitor_ids: [1, 2, 3, 4, 5] },
      { day_start_at: day(13), monitor_ids: [6, 7] },
    ]);

    const selfFetch = vi.mocked(env.SELF!.fetch);
    expect(selfFetch).toHaveBeenCalledTimes(6);
    for (const call of selfFetch.mock.calls) {
      const request = call[0] as Request;
      expect(new URL(request.url).pathname).toBe('/api/v1/internal/rollup/daily');
      expect(request.headers.get('Authorization')).toBe('Bearer test-admin-token');
    }
    expect(warnSpy).not.toHaveBeenCalled();
  });

  it('throws for inline fallback when every service call fails', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const env = createEnv(async () => {
      throw new Error('d1 down');
    });

    await expect(
      runDailyRollupViaService(
        env,
        { scheduledTime: Date.UTC(2026, 0, 18, 0, 0, 0) } as ScheduledController,
      ),
    ).rejects.toThrow('daily rollup: all service calls failed');
    // One failed chunk per day aborts that day; all 3 days attempted.
    expect(warnSpy).toHaveBeenCalledTimes(3);
  });

  it('refuses the service path without a SELF binding', async () => {
    const env = createEnv(async () => new Response('{}', { status: 200 }));
    delete env.SELF;

    await expect(
      runDailyRollupViaService(
        env,
        { scheduledTime: Date.UTC(2026, 0, 18, 0, 0, 0) } as ScheduledController,
      ),
    ).rejects.toThrow('SELF service binding missing');
  });

  it('refuses the service path when the flag is disabled', async () => {
    const env = createEnv(async () => new Response('{}', { status: 200 }));
    env.UPTIMER_SCHEDULED_ROLLUP_VIA_SERVICE = '0';

    await expect(
      runDailyRollupViaService(
        env,
        { scheduledTime: Date.UTC(2026, 0, 18, 0, 0, 0) } as ScheduledController,
      ),
    ).rejects.toThrow('disabled by flag');
  });
});
