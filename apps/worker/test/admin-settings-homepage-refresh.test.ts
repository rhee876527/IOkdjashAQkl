import { afterEach, describe, expect, it, vi } from 'vitest';

vi.mock('../src/snapshots', () => ({
  refreshPublicHomepageSnapshotIfNeeded: vi.fn(),
}));

import type { Env } from '../src/env';
import { adminSettingsRoutes } from '../src/routes/admin-settings';
import { refreshPublicHomepageSnapshotIfNeeded } from '../src/snapshots';
import { createFakeD1Database, type FakeD1QueryHandler } from './helpers/fake-d1';

describe('admin settings homepage snapshot refresh', () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it('queues a homepage snapshot refresh after settings writes', async () => {
    const settingsMap = new Map<string, string>([
      ['site_title', 'Uptimer'],
      ['site_description', ''],
      ['site_locale', 'auto'],
      ['site_timezone', 'UTC'],
      ['retention_check_results_days', '7'],
      ['state_failures_to_down_from_up', '2'],
      ['state_successes_to_up_from_down', '2'],
      ['admin_default_overview_range', '24h'],
      ['admin_default_monitor_range', '24h'],
      ['uptime_rating_level', '3'],
    ]);

    const handlers: FakeD1QueryHandler[] = [
      {
        match: 'insert into settings',
        run: (args) => {
          settingsMap.set(String(args[0]), String(args[1]));
          return { meta: { changes: 1 } };
        },
      },
      {
        match: 'select key, value from settings',
        all: () =>
          [...settingsMap.entries()].map(([key, value]) => ({
            key,
            value,
          })),
      },
    ];

    vi.mocked(refreshPublicHomepageSnapshotIfNeeded).mockResolvedValue(false);

    const env = {
      DB: createFakeD1Database(handlers),
    } as unknown as Env;
    const waitUntil = vi.fn();

    const res = await adminSettingsRoutes.fetch(
      new Request('https://status.example.com/', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ site_title: 'Status Hub' }),
      }),
      env,
      { waitUntil } as unknown as ExecutionContext,
    );

    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({
      settings: {
        site_title: 'Status Hub',
      },
    });
    expect(waitUntil).toHaveBeenCalledTimes(1);
    await Promise.all(waitUntil.mock.calls.map((call) => call[0] as Promise<unknown>));
    expect(refreshPublicHomepageSnapshotIfNeeded).toHaveBeenCalledTimes(1);
  });
});
