import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Hono } from 'hono';

import type { Env } from '../src/env';
import { handleError, handleNotFound } from '../src/middleware/errors';
import { publicRoutes } from '../src/routes/public';
import { createFakeD1Database, type FakeD1QueryHandler } from './helpers/fake-d1';

type CacheStore = Map<string, Response>;

function installCacheMock(store: CacheStore) {
  const open = vi.fn(async () => ({
    async match(request: Request) {
      const cached = store.get(request.url);
      return cached ? cached.clone() : undefined;
    },
    async put(request: Request, response: Response) {
      store.set(request.url, response.clone());
    },
  }));

  Object.defineProperty(globalThis, 'caches', {
    configurable: true,
    value: { open },
  });
}

async function requestHomepage(handlers: FakeD1QueryHandler[]) {
  const env = {
    DB: createFakeD1Database(handlers),
    ADMIN_TOKEN: 'test-admin-token',
  } as unknown as Env;

  const app = new Hono<{ Bindings: Env }>();
  app.onError(handleError);
  app.notFound(handleNotFound);
  app.route('/api/v1/public', publicRoutes);

  return app.fetch(
    new Request('https://status.example.com/api/v1/public/homepage'),
    env,
    { waitUntil: vi.fn() } as unknown as ExecutionContext,
  );
}

function samplePayload(now = 1_728_000_000) {
  return {
    generated_at: now,
    site_title: 'Uptimer',
    site_description: '',
    site_locale: 'auto',
    site_timezone: 'UTC',
    uptime_rating_level: 3,
    overall_status: 'up',
    banner: {
      source: 'monitors',
      status: 'operational',
      title: 'All Systems Operational',
      down_ratio: null,
    },
    summary: {
      up: 1,
      down: 0,
      maintenance: 0,
      paused: 0,
      unknown: 0,
    },
    monitors: [],
    active_incidents: [],
    maintenance_windows: {
      active: [],
      upcoming: [],
    },
    resolved_incident_preview: null,
    maintenance_history_preview: null,
  };
}

describe('public homepage route', () => {
  const originalCaches = (globalThis as { caches?: unknown }).caches;

  beforeEach(() => {
    installCacheMock(new Map());
  });

  afterEach(() => {
    if (originalCaches === undefined) {
      delete (globalThis as { caches?: unknown }).caches;
    } else {
      Object.defineProperty(globalThis, 'caches', {
        configurable: true,
        value: originalCaches,
      });
    }
    vi.restoreAllMocks();
  });

  it('serves a fresh homepage snapshot without live compute', async () => {
    const payload = samplePayload(190);
    vi.spyOn(Date, 'now').mockReturnValue(200_000);

    const res = await requestHomepage([
      {
        match: 'from public_snapshots',
        first: () => ({
          generated_at: payload.generated_at,
          body_json: JSON.stringify(payload),
        }),
      },
    ]);

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual(payload);
  });

  it('serves a bounded stale homepage snapshot instead of computing in-request', async () => {
    const payload = samplePayload(100);
    vi.spyOn(Date, 'now').mockReturnValue(200_000);

    const res = await requestHomepage([
      {
        match: 'from public_snapshots',
        first: () => ({
          generated_at: payload.generated_at,
          body_json: JSON.stringify(payload),
        }),
      },
    ]);

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual(payload);
    expect(res.headers.get('Cache-Control')).toBe(
      'public, max-age=0, stale-while-revalidate=0, stale-if-error=0',
    );
  });

  it('returns 503 when no homepage snapshot is available', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(200_000);

    const res = await requestHomepage([
      {
        match: 'from public_snapshots',
        first: () => null,
      },
    ]);

    expect(res.status).toBe(503);
    expect(await res.json()).toMatchObject({
      error: {
        code: 'UNAVAILABLE',
      },
    });
  });
});
