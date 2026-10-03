import { afterEach, describe, expect, it, vi } from 'vitest';

const runDailyRollup = vi.fn();
const runDailyRollupViaService = vi.fn();
const runRetention = vi.fn();
const runScheduledTick = vi.fn();

vi.mock('../src/scheduler/daily-rollup', () => ({
  runDailyRollup,
}));

vi.mock('../src/scheduler/retention', () => ({
  runRetention,
}));

vi.mock('../src/scheduler/scheduled', () => ({
  runDailyRollupViaService,
  runScheduledTick,
}));

import worker from '../src/index';
import type { Env } from '../src/env';

function createExecutionContext(): {
  ctx: ExecutionContext;
  waitUntil: ReturnType<typeof vi.fn>;
  waitUntilPromises: Promise<unknown>[];
} {
  const waitUntilPromises: Promise<unknown>[] = [];
  const waitUntil = vi.fn((promise: Promise<unknown>) => {
    waitUntilPromises.push(promise);
  });
  return {
    ctx: { waitUntil } as unknown as ExecutionContext,
    waitUntil,
    waitUntilPromises,
  };
}

describe('worker scheduled dispatch', () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it('runs only the minute scheduler for ordinary consolidated cron ticks', async () => {
    const controller = {
      cron: '* * * * *',
      scheduledTime: Date.UTC(2026, 1, 18, 0, 1, 0),
    } as ScheduledController;
    const env = {} as Env;
    const { ctx, waitUntil } = createExecutionContext();

    await worker.scheduled(controller, env, ctx);

    expect(runScheduledTick).toHaveBeenCalledWith(env, ctx);
    expect(runDailyRollupViaService).not.toHaveBeenCalled();
    expect(runDailyRollup).not.toHaveBeenCalled();
    expect(runRetention).not.toHaveBeenCalled();
    expect(waitUntil).not.toHaveBeenCalled();
  });

  it('runs the daily rollup through the service path at UTC midnight', async () => {
    const controller = {
      cron: '* * * * *',
      scheduledTime: Date.UTC(2026, 1, 18, 0, 0, 0),
    } as ScheduledController;
    const env = {} as Env;
    const { ctx, waitUntil, waitUntilPromises } = createExecutionContext();

    await worker.scheduled(controller, env, ctx);
    await Promise.all(waitUntilPromises);

    expect(runScheduledTick).toHaveBeenCalledWith(env, ctx);
    expect(waitUntil).toHaveBeenCalledTimes(1);
    expect(runDailyRollupViaService).toHaveBeenCalledWith(env, controller);
    expect(runDailyRollup).not.toHaveBeenCalled();
    expect(runRetention).not.toHaveBeenCalled();
  });

  it('falls back to the inline rollup when the service path fails', async () => {
    runDailyRollupViaService.mockRejectedValueOnce(new Error('service down'));
    const controller = {
      cron: '* * * * *',
      scheduledTime: Date.UTC(2026, 1, 18, 1, 0, 0),
    } as ScheduledController;
    const env = {} as Env;
    const { ctx, waitUntilPromises } = createExecutionContext();

    await worker.scheduled(controller, env, ctx);
    await Promise.all(waitUntilPromises);

    expect(runDailyRollupViaService).toHaveBeenCalledWith(env, controller);
    expect(runDailyRollup).toHaveBeenCalledWith(env, controller, ctx);
  });

  it('runs the daily rollup on the widened 00:00-04:00 UTC window', async () => {
    for (const hour of [2, 3, 4]) {
      vi.clearAllMocks();
      const controller = {
        cron: '* * * * *',
        scheduledTime: Date.UTC(2026, 1, 18, hour, 0, 0),
      } as ScheduledController;
      const env = {} as Env;
      const { ctx, waitUntilPromises } = createExecutionContext();

      await worker.scheduled(controller, env, ctx);
      await Promise.all(waitUntilPromises);

      expect(runDailyRollupViaService).toHaveBeenCalledWith(env, controller);
    }

    vi.clearAllMocks();
    const controller = {
      cron: '* * * * *',
      scheduledTime: Date.UTC(2026, 1, 18, 5, 0, 0),
    } as ScheduledController;
    const env = {} as Env;
    const { ctx, waitUntilPromises } = createExecutionContext();

    await worker.scheduled(controller, env, ctx);
    await Promise.all(waitUntilPromises);

    expect(runDailyRollupViaService).not.toHaveBeenCalled();
    expect(runDailyRollup).not.toHaveBeenCalled();
  });

  it('queues retention at UTC 00:30 on the consolidated minute cron', async () => {
    const controller = {
      cron: '* * * * *',
      scheduledTime: Date.UTC(2026, 1, 18, 0, 30, 0),
    } as ScheduledController;
    const env = {} as Env;
    const { ctx, waitUntil, waitUntilPromises } = createExecutionContext();

    await worker.scheduled(controller, env, ctx);
    await Promise.all(waitUntilPromises);

    expect(runScheduledTick).toHaveBeenCalledWith(env, ctx);
    expect(waitUntil).toHaveBeenCalledTimes(1);
    expect(runRetention).toHaveBeenCalledWith(env, controller);
    expect(runDailyRollupViaService).not.toHaveBeenCalled();
    expect(runDailyRollup).not.toHaveBeenCalled();
  });

  it('keeps the legacy daily rollup cron compatible during trigger propagation', async () => {
    const controller = {
      cron: '0 0 * * *',
      scheduledTime: Date.UTC(2026, 1, 18, 0, 0, 0),
    } as ScheduledController;
    const env = {} as Env;
    const { ctx, waitUntil, waitUntilPromises } = createExecutionContext();

    await worker.scheduled(controller, env, ctx);
    await Promise.all(waitUntilPromises);

    expect(waitUntil).toHaveBeenCalledTimes(1);
    expect(runDailyRollupViaService).toHaveBeenCalledWith(env, controller);
    expect(runDailyRollup).not.toHaveBeenCalled();
    expect(runRetention).not.toHaveBeenCalled();
    expect(runScheduledTick).not.toHaveBeenCalled();
  });

  it('keeps the legacy retention cron compatible during trigger propagation', async () => {
    const controller = {
      cron: '30 0 * * *',
      scheduledTime: Date.UTC(2026, 1, 18, 0, 30, 0),
    } as ScheduledController;
    const env = {} as Env;
    const { ctx, waitUntil, waitUntilPromises } = createExecutionContext();

    await worker.scheduled(controller, env, ctx);
    await Promise.all(waitUntilPromises);

    expect(waitUntil).toHaveBeenCalledTimes(1);
    expect(runRetention).toHaveBeenCalledWith(env, controller);
    expect(runDailyRollupViaService).not.toHaveBeenCalled();
    expect(runDailyRollup).not.toHaveBeenCalled();
    expect(runScheduledTick).not.toHaveBeenCalled();
  });
});
