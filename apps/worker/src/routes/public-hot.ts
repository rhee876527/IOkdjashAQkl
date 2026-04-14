import { Hono } from 'hono';

import type { Env } from '../env';
import { hasValidAdminTokenRequest } from '../middleware/auth';
import { AppError } from '../middleware/errors';
import {
  Trace,
  applyTraceToResponse,
  resolveTraceOptions,
} from '../observability/trace';
import {
  applyHomepageCacheHeaders,
  readHomepageSnapshotJsonAnyAge,
  readHomepageSnapshotArtifactJson,
  readStaleHomepageSnapshotArtifactJson,
} from '../snapshots/public-homepage-read';
import {
  applyStatusCacheHeaders,
  readStatusSnapshotJson,
  readStaleStatusSnapshotJson,
} from '../snapshots/public-status-read';

const HOMEPAGE_STALE_GRACE_SECONDS = 2 * 60;

function appendVaryHeader(res: Response, value: string): void {
  const next = value.trim();
  if (!next) return;
  const existing = res.headers.get('Vary');
  if (!existing) {
    res.headers.set('Vary', next);
    return;
  }
  const parts = existing.split(',').map((part) => part.trim().toLowerCase());
  if (parts.includes(next.toLowerCase())) return;
  res.headers.set('Vary', `${existing}, ${next}`);
}

function applyCorsHeaders(res: Response, origin: string | null): Response {
  if (!origin) return res;
  const out = new Response(res.body, res);
  out.headers.set('Access-Control-Allow-Origin', origin);
  appendVaryHeader(out, 'Origin');
  out.headers.set('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
  out.headers.set('Access-Control-Allow-Headers', 'Authorization,Content-Type');
  return out;
}

function applyPrivateNoStore(res: Response): Response {
  const vary = res.headers.get('Vary');
  if (!vary) {
    res.headers.set('Vary', 'Authorization');
  } else if (!vary.split(',').some((part) => part.trim().toLowerCase() === 'authorization')) {
    res.headers.set('Vary', `${vary}, Authorization`);
  }

  res.headers.set('Cache-Control', 'private, no-store');
  return res;
}

function rewritePublicRequest(req: Request): Request {
  const url = new URL(req.url);
  const prefix = '/api/v1/public';
  if (url.pathname === prefix) {
    url.pathname = '/';
  } else if (url.pathname.startsWith(`${prefix}/`)) {
    url.pathname = url.pathname.slice(prefix.length);
  }
  return new Request(url.toString(), req);
}

export const publicHotRoutes = new Hono<{ Bindings: Env }>();

publicHotRoutes.get('/homepage', async (c) => {
  const now = Math.floor(Date.now() / 1000);
  const trace = new Trace(
    resolveTraceOptions({
      header: (name) => c.req.header(name),
      env: c.env as unknown as Record<string, unknown>,
    }),
  );
  trace.setLabel('route', 'public/homepage');

  const snapshot = await trace.timeAsync('homepage_snapshot_read', () =>
    readHomepageSnapshotJsonAnyAge(c.env.DB, now, HOMEPAGE_STALE_GRACE_SECONDS),
  );
  if (snapshot) {
    c.header('Content-Type', 'application/json; charset=utf-8');
    const res = c.body(snapshot.bodyJson);
    applyHomepageCacheHeaders(res, Math.min(60, snapshot.age));
    trace.setLabel('path', snapshot.age <= 60 ? 'snapshot' : 'stale');
    trace.setLabel('age', snapshot.age);
    trace.finish('total');
    applyTraceToResponse({ res, trace, prefix: 'w' });
    return res;
  }

  const { publicRoutes } = await import('./public');
  const res = await publicRoutes.fetch(rewritePublicRequest(c.req.raw), c.env, c.executionCtx);
  return applyCorsHeaders(res, c.req.header('Origin') ?? null);
});

publicHotRoutes.get('/homepage-artifact', async (c) => {
  const now = Math.floor(Date.now() / 1000);
  const trace = new Trace(
    resolveTraceOptions({
      header: (name) => c.req.header(name),
      env: c.env as unknown as Record<string, unknown>,
    }),
  );
  trace.setLabel('route', 'public/homepage-artifact');

  const snapshot = await trace.timeAsync('homepage_artifact_read', () =>
    readHomepageSnapshotArtifactJson(c.env.DB, now),
  );
  if (snapshot) {
    c.header('Content-Type', 'application/json; charset=utf-8');
    const res = c.body(snapshot.bodyJson);
    applyHomepageCacheHeaders(res, snapshot.age);
    trace.setLabel('path', 'snapshot');
    trace.setLabel('age', snapshot.age);
    trace.setLabel('bytes', snapshot.bodyJson.length);
    trace.finish('total');
    applyTraceToResponse({ res, trace, prefix: 'w' });
    return res;
  }

  const stale = await trace.timeAsync('homepage_artifact_stale_read', () =>
    readStaleHomepageSnapshotArtifactJson(c.env.DB, now),
  );
  if (stale) {
    c.header('Content-Type', 'application/json; charset=utf-8');
    const res = c.body(stale.bodyJson);
    applyHomepageCacheHeaders(res, Math.min(60, stale.age));
    trace.setLabel('path', 'stale');
    trace.setLabel('age', stale.age);
    trace.setLabel('bytes', stale.bodyJson.length);
    trace.finish('total');
    applyTraceToResponse({ res, trace, prefix: 'w' });
    return res;
  }

  throw new AppError(503, 'UNAVAILABLE', 'Homepage artifact unavailable');
});

publicHotRoutes.get('/status', async (c) => {
  const now = Math.floor(Date.now() / 1000);
  const includeHiddenMonitors = hasValidAdminTokenRequest(c);
  const trace = new Trace(
    resolveTraceOptions({
      header: (name) => c.req.header(name),
      env: c.env as unknown as Record<string, unknown>,
    }),
  );
  trace.setLabel('route', 'public/status');
  trace.setLabel('hidden', includeHiddenMonitors);

  if (includeHiddenMonitors) {
    const { computePublicStatusPayload } = await import('../public/status');
    const payload = await trace.timeAsync('status_compute', () =>
      computePublicStatusPayload(c.env.DB, now, { includeHiddenMonitors: true }),
    );
    const res = applyPrivateNoStore(c.json(payload));
    trace.finish('total');
    applyTraceToResponse({ res, trace, prefix: 'w' });
    return res;
  }

  const snapshot = await trace.timeAsync('status_snapshot_read', () =>
    readStatusSnapshotJson(c.env.DB, now),
  );
  if (snapshot) {
    c.header('Content-Type', 'application/json; charset=utf-8');
    const res = c.body(snapshot.bodyJson);
    applyStatusCacheHeaders(res, snapshot.age);
    trace.setLabel('path', 'snapshot');
    trace.setLabel('age', snapshot.age);
    trace.finish('total');
    applyTraceToResponse({ res, trace, prefix: 'w' });
    return res;
  }

  try {
    const [{ computePublicStatusPayload }, { writeStatusSnapshot }] = await Promise.all([
      import('../public/status'),
      import('../snapshots/public-status'),
    ]);

    const payload = await trace.timeAsync('status_compute', () =>
      computePublicStatusPayload(c.env.DB, now),
    );
    const res = c.json(payload);
    applyStatusCacheHeaders(res, 0);

    c.executionCtx.waitUntil(
      writeStatusSnapshot(c.env.DB, now, payload).catch((err) => {
        console.warn('public snapshot: write failed', err);
      }),
    );

    trace.setLabel('path', 'compute');
    trace.finish('total');
    applyTraceToResponse({ res, trace, prefix: 'w' });
    return res;
  } catch (err) {
    console.warn('public status: compute failed', err);

    // Last-resort fallback: serve a bounded stale snapshot instead of failing entirely.
    const stale = await trace.timeAsync('status_snapshot_stale_read', () =>
      readStaleStatusSnapshotJson(c.env.DB, now, 10 * 60),
    );
    if (stale) {
      c.header('Content-Type', 'application/json; charset=utf-8');
      const res = c.body(stale.bodyJson);
      applyStatusCacheHeaders(res, Math.min(60, stale.age));
      trace.setLabel('path', 'stale');
      trace.setLabel('age', stale.age);
      trace.finish('total');
      applyTraceToResponse({ res, trace, prefix: 'w' });
      return res;
    }

    throw err;
  }
});

// Everything else stays behind a lazy import, keeping cold-start CPU focused on the
// homepage/status hot paths.
publicHotRoutes.all('*', async (c) => {
  const { publicRoutes } = await import('./public');
  const res = await publicRoutes.fetch(rewritePublicRequest(c.req.raw), c.env, c.executionCtx);
  return applyCorsHeaders(res, c.req.header('Origin') ?? null);
});
