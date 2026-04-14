import type { Env } from './env';
import { AppError } from './middleware/errors';
import type { Trace } from './observability/trace';
import {
  applyHomepageCacheHeaders,
  readHomepageSnapshotArtifactJson,
  readHomepageSnapshotJsonAnyAge,
  readStaleHomepageSnapshotArtifactJson,
} from './snapshots/public-homepage-read';
import {
  applyStatusCacheHeaders,
  readStatusSnapshotJson,
  readStaleStatusSnapshotJson,
} from './snapshots/public-status-read';

const CORS_ALLOW_METHODS = 'GET,POST,PUT,PATCH,DELETE,OPTIONS';
const CORS_ALLOW_HEADERS = 'Authorization,Content-Type';
const HOMEPAGE_STALE_GRACE_SECONDS = 2 * 60;

function appendVaryHeader(headers: Headers, value: string): void {
  const next = value.trim();
  if (!next) return;
  const existing = headers.get('Vary');
  if (!existing) {
    headers.set('Vary', next);
    return;
  }
  const parts = existing.split(',').map((part) => part.trim().toLowerCase());
  if (parts.includes(next.toLowerCase())) return;
  headers.set('Vary', `${existing}, ${next}`);
}

function applyCorsHeaders(res: Response, origin: string | null): Response {
  if (!origin) return res;
  const out = new Response(res.body, res);
  out.headers.set('Access-Control-Allow-Origin', origin);
  appendVaryHeader(out.headers, 'Origin');
  out.headers.set('Access-Control-Allow-Methods', CORS_ALLOW_METHODS);
  out.headers.set('Access-Control-Allow-Headers', CORS_ALLOW_HEADERS);
  return out;
}

function corsPreflight(origin: string | null): Response {
  const res = new Response(null, { status: 204 });
  if (origin) {
    res.headers.set('Access-Control-Allow-Origin', origin);
    res.headers.set('Vary', 'Origin');
  }
  res.headers.set('Access-Control-Allow-Methods', CORS_ALLOW_METHODS);
  res.headers.set('Access-Control-Allow-Headers', CORS_ALLOW_HEADERS);
  return res;
}

function isZodErrorLike(err: unknown): err is { message: string } {
  if (!err || typeof err !== 'object') return false;
  const record = err as Record<string, unknown>;
  return (
    record['name'] === 'ZodError' &&
    typeof record['message'] === 'string' &&
    Array.isArray(record['issues'])
  );
}

function jsonError(status: number, code: string, message: string): Response {
  return new Response(JSON.stringify({ error: { code, message } }), {
    status,
    headers: { 'Content-Type': 'application/json; charset=utf-8' },
  });
}

function readBearerToken(authHeader: string | null): string | null {
  if (!authHeader) return null;
  const match = authHeader.match(/^Bearer\\s+(.+)$/i);
  return match?.[1] ?? null;
}

function hasValidAdminToken(req: Request, env: Pick<Env, 'ADMIN_TOKEN'>): boolean {
  const expected = env.ADMIN_TOKEN;
  if (!expected) return false;
  return readBearerToken(req.headers.get('authorization')) === expected;
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

function normalizeTruthyHeader(value: string | null): boolean {
  if (!value) return false;
  const normalized = value.trim().toLowerCase();
  if (!normalized) return false;
  return normalized === '1' || normalized === 'true' || normalized === 'yes' || normalized === 'on';
}

async function resolveTrace(req: Request, env: Env): Promise<Trace | null> {
  if (!normalizeTruthyHeader(req.headers.get('X-Uptimer-Trace'))) return null;
  const mod = await import('./observability/trace');
  const trace = new mod.Trace(
    mod.resolveTraceOptions({
      header: (name) => req.headers.get(name) ?? undefined,
      env: env as unknown as Record<string, unknown>,
    }),
  );
  return trace as Trace;
}

async function applyTrace(res: Response, trace: Trace | null, prefix: string): Promise<void> {
  if (!trace) return;
  const mod = await import('./observability/trace');
  mod.applyTraceToResponse({ res, trace, prefix });
}

async function handlePublicHomepageArtifact(req: Request, env: Env): Promise<Response> {
  const now = Math.floor(Date.now() / 1000);
  const trace = await resolveTrace(req, env);
  if (trace) {
    trace.setLabel('route', 'public/homepage-artifact');
  }

  const snapshot = trace
    ? await trace.timeAsync(
        'homepage_artifact_read',
        () => readHomepageSnapshotArtifactJson(env.DB, now),
      )
    : await readHomepageSnapshotArtifactJson(env.DB, now);
  if (snapshot) {
    const res = new Response(snapshot.bodyJson, {
      status: 200,
      headers: { 'Content-Type': 'application/json; charset=utf-8' },
    });
    applyHomepageCacheHeaders(res, snapshot.age);
    if (trace) {
      trace.setLabel('path', 'snapshot');
      trace.setLabel('age', snapshot.age);
      trace.setLabel('bytes', snapshot.bodyJson.length);
      trace.finish('total');
      await applyTrace(res, trace, 'w');
    }
    return res;
  }

  const stale = trace
    ? await trace.timeAsync(
        'homepage_artifact_stale_read',
        () => readStaleHomepageSnapshotArtifactJson(env.DB, now),
      )
    : await readStaleHomepageSnapshotArtifactJson(env.DB, now);
  if (stale) {
    const res = new Response(stale.bodyJson, {
      status: 200,
      headers: { 'Content-Type': 'application/json; charset=utf-8' },
    });
    applyHomepageCacheHeaders(res, Math.min(60, stale.age));
    if (trace) {
      trace.setLabel('path', 'stale');
      trace.setLabel('age', stale.age);
      trace.setLabel('bytes', stale.bodyJson.length);
      trace.finish('total');
      await applyTrace(res, trace, 'w');
    }
    return res;
  }

  throw new AppError(503, 'UNAVAILABLE', 'Homepage artifact unavailable');
}

async function handlePublicHomepage(req: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  const now = Math.floor(Date.now() / 1000);
  const trace = await resolveTrace(req, env);
  if (trace) {
    trace.setLabel('route', 'public/homepage');
  }

  const snapshot = trace
    ? await trace.timeAsync(
        'homepage_snapshot_read',
        () => readHomepageSnapshotJsonAnyAge(env.DB, now, HOMEPAGE_STALE_GRACE_SECONDS),
      )
    : await readHomepageSnapshotJsonAnyAge(env.DB, now, HOMEPAGE_STALE_GRACE_SECONDS);
  if (snapshot) {
    const res = new Response(snapshot.bodyJson, {
      status: 200,
      headers: { 'Content-Type': 'application/json; charset=utf-8' },
    });
    applyHomepageCacheHeaders(res, Math.min(60, snapshot.age));
    if (trace) {
      trace.setLabel('path', snapshot.age <= 60 ? 'snapshot' : 'stale');
      trace.setLabel('age', snapshot.age);
      trace.finish('total');
      await applyTrace(res, trace, 'w');
    }
    return res;
  }

  const { publicRoutes } = await import('./routes/public');
  return publicRoutes.fetch(rewritePublicRequest(req), env, ctx);
}

async function handlePublicStatus(req: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  const now = Math.floor(Date.now() / 1000);
  const includeHiddenMonitors = hasValidAdminToken(req, env);
  const trace = await resolveTrace(req, env);
  if (trace) {
    trace.setLabel('route', 'public/status');
    trace.setLabel('hidden', includeHiddenMonitors);
  }

  if (includeHiddenMonitors) {
    const { computePublicStatusPayload } = await import('./public/status');
    const payload = trace
      ? await trace.timeAsync(
          'status_compute',
          () => computePublicStatusPayload(env.DB, now, { includeHiddenMonitors: true }),
        )
      : await computePublicStatusPayload(env.DB, now, { includeHiddenMonitors: true });

    const res = applyPrivateNoStore(
      new Response(JSON.stringify(payload), {
        status: 200,
        headers: { 'Content-Type': 'application/json; charset=utf-8' },
      }),
    );
    if (trace) {
      trace.finish('total');
      await applyTrace(res, trace, 'w');
    }
    return res;
  }

  const snapshot = trace
    ? await trace.timeAsync(
        'status_snapshot_read',
        () => readStatusSnapshotJson(env.DB, now),
      )
    : await readStatusSnapshotJson(env.DB, now);
  if (snapshot) {
    const res = new Response(snapshot.bodyJson, {
      status: 200,
      headers: { 'Content-Type': 'application/json; charset=utf-8' },
    });
    applyStatusCacheHeaders(res, snapshot.age);
    if (trace) {
      trace.setLabel('path', 'snapshot');
      trace.setLabel('age', snapshot.age);
      trace.finish('total');
      await applyTrace(res, trace, 'w');
    }
    return res;
  }

  try {
    const [{ computePublicStatusPayload }, { writeStatusSnapshot }] = await Promise.all([
      import('./public/status'),
      import('./snapshots/public-status'),
    ]);
    const payload = trace
      ? await trace.timeAsync(
          'status_compute',
          () => computePublicStatusPayload(env.DB, now),
        )
      : await computePublicStatusPayload(env.DB, now);

    const res = new Response(JSON.stringify(payload), {
      status: 200,
      headers: { 'Content-Type': 'application/json; charset=utf-8' },
    });
    applyStatusCacheHeaders(res, 0);

    ctx.waitUntil(
      writeStatusSnapshot(env.DB, now, payload).catch((err) => {
        console.warn('public snapshot: write failed', err);
      }),
    );

    if (trace) {
      trace.setLabel('path', 'compute');
      trace.finish('total');
      await applyTrace(res, trace, 'w');
    }
    return res;
  } catch (err) {
    console.warn('public status: compute failed', err);

    const stale = trace
      ? await trace.timeAsync(
          'status_snapshot_stale_read',
          () => readStaleStatusSnapshotJson(env.DB, now, 10 * 60),
        )
      : await readStaleStatusSnapshotJson(env.DB, now, 10 * 60);
    if (stale) {
      const res = new Response(stale.bodyJson, {
        status: 200,
        headers: { 'Content-Type': 'application/json; charset=utf-8' },
      });
      applyStatusCacheHeaders(res, Math.min(60, stale.age));
      if (trace) {
        trace.setLabel('path', 'stale');
        trace.setLabel('age', stale.age);
        trace.finish('total');
        await applyTrace(res, trace, 'w');
      }
      return res;
    }

    throw err;
  }
}

export async function handleFetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  const url = new URL(request.url);
  const origin = request.headers.get('Origin');

  if (url.pathname === '/') {
    return new Response('ok');
  }

  if (url.pathname.startsWith('/api/')) {
    if (request.method === 'OPTIONS') {
      return corsPreflight(origin);
    }

    // Redirect legacy `/api/*` paths to the versioned API.
    if (!(url.pathname === '/api/v1' || url.pathname.startsWith('/api/v1/'))) {
      const next = new URL(request.url);
      next.pathname = `/api/v1${url.pathname.slice('/api'.length)}`;
      const res = Response.redirect(next.toString(), 308);
      return applyCorsHeaders(res, origin);
    }
  }

  try {
    if (url.pathname === '/api/v1/public/homepage-artifact') {
      const res = await handlePublicHomepageArtifact(request, env);
      return applyCorsHeaders(res, origin);
    }
    if (url.pathname === '/api/v1/public/homepage') {
      const res = await handlePublicHomepage(request, env, ctx);
      return applyCorsHeaders(res, origin);
    }
    if (url.pathname === '/api/v1/public/status') {
      const res = await handlePublicStatus(request, env, ctx);
      return applyCorsHeaders(res, origin);
    }
  } catch (err) {
    if (err instanceof AppError) {
      return applyCorsHeaders(jsonError(err.status, err.code, err.message), origin);
    }
    if (isZodErrorLike(err)) {
      return applyCorsHeaders(jsonError(400, 'INVALID_ARGUMENT', err.message), origin);
    }
    console.error(err);
    return applyCorsHeaders(jsonError(500, 'INTERNAL', 'Internal Server Error'), origin);
  }

  // Everything else stays behind a lazy import to keep cold-start CPU focused on the hot paths.
  const { fetch } = await import('./hono-app');
  return fetch(request, env, ctx);
}
