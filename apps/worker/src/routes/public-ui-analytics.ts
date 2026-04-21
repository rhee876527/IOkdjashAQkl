import { Hono } from 'hono';
import { z } from 'zod';

import { utcDayStart } from '../analytics/uptime';
import type { Env } from '../env';
import { hasValidAdminTokenRequest } from '../middleware/auth';
import { cachePublic } from '../middleware/cache-public';
import { Trace, applyTraceToResponse, resolveTraceOptions } from '../observability/trace';
import {
  analyticsOverviewSnapshotSupportsMonitors,
  readPublicAnalyticsOverviewSnapshot,
  refreshPublicAnalyticsOverviewSnapshotIfNeeded,
  totalsFromAnalyticsOverviewEntry,
  toPublicAnalyticsOverviewEntryMap,
} from '../public/analytics-overview';
import {
  materializeMonitorRuntimeTotals,
  readPublicMonitorRuntimeTotalsSnapshot,
  toMonitorRuntimeTotalsEntryMap,
} from '../public/monitor-runtime';
import { monitorVisibilityPredicate } from '../public/visibility';

const uptimeOverviewRangeSchema = z.enum(['30d', '90d']);

function isAuthorizedStatusAdminRequest(c: {
  env: Pick<Env, 'ADMIN_TOKEN'>;
  req: { header(name: string): string | undefined };
}): boolean {
  return hasValidAdminTokenRequest(c);
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

function withVisibilityAwareCaching(res: Response, includeHiddenMonitors: boolean): Response {
  return includeHiddenMonitors ? applyPrivateNoStore(res) : res;
}

function createTrace(c: {
  env: Env;
  req: { header(name: string): string | undefined };
}): Trace {
  return new Trace(
    resolveTraceOptions({
      header: (name) => c.req.header(name),
      env: c.env as unknown as Record<string, unknown>,
    }),
  );
}

function normalizeAnalyticsUptimeCacheKeyUrl(url: URL): void {
  const range = url.searchParams.get('range');
  if (range !== null && range !== '30d' && range !== '90d') {
    return;
  }

  url.search = '';
  if (range === '90d') {
    url.searchParams.set('range', '90d');
  }
}

const statementCacheByDb = new WeakMap<D1Database, Map<string, D1PreparedStatement>>();

function prepareStatement(db: D1Database, sql: string): D1PreparedStatement {
  let statements = statementCacheByDb.get(db);
  if (!statements) {
    statements = new Map<string, D1PreparedStatement>();
    statementCacheByDb.set(db, statements);
  }

  const cached = statements.get(sql);
  if (cached) {
    return cached;
  }

  const statement = db.prepare(sql);
  statements.set(sql, statement);
  return statement;
}

export async function handlePublicAnalyticsUptime(c: {
  env: Env;
  req: {
    query(name: string): string | undefined;
    raw: Request;
    header(name: string): string | undefined;
  };
  executionCtx: ExecutionContext;
  json: (data: unknown) => Response;
}): Promise<Response> {
  const includeHiddenMonitors = isAuthorizedStatusAdminRequest(c);
  const range = uptimeOverviewRangeSchema.optional().default('30d').parse(c.req.query('range'));
  const trace = createTrace(c);
  trace.setLabel('route', 'public/analytics-uptime');
  trace.setLabel('range', range);

  const now = Math.floor(Date.now() / 1000);
  const rangeEnd = Math.floor(now / 60) * 60;
  const rangeEndFullDays = utcDayStart(rangeEnd);
  const rangeStart = rangeEnd - (range === '30d' ? 30 * 86400 : 90 * 86400);

  const monitorRowsPromise = trace.timeAsync(
    'active_monitors',
    async () =>
      await prepareStatement(
        c.env.DB,
        `
          SELECT m.id, m.name, m.type, m.created_at
          FROM monitors m
          WHERE m.is_active = 1
            AND ${monitorVisibilityPredicate(includeHiddenMonitors, 'm')}
          ORDER BY m.id
        `,
      )
        .all<{
          id: number;
          name: string;
          type: string;
          created_at: number;
        }>(),
  );
  const historySnapshotPromise = trace.timeAsync(
    'history_snapshot',
    async () => await readPublicAnalyticsOverviewSnapshot(c.env.DB, rangeEndFullDays),
  );
  const [{ results: monitorRows }, historySnapshot] = await Promise.all([
    monitorRowsPromise,
    historySnapshotPromise,
  ]);

  const monitors = monitorRows ?? [];
  if (
    monitors.length > 0 &&
    (!historySnapshot || !analyticsOverviewSnapshotSupportsMonitors(historySnapshot, monitors))
  ) {
    c.executionCtx.waitUntil(
      refreshPublicAnalyticsOverviewSnapshotIfNeeded({
        db: c.env.DB,
        now,
        fullDayEndAt: rangeEndFullDays,
        force: historySnapshot !== null,
      }).catch((err) => {
        console.warn('analytics overview: background refresh failed', err);
      }),
    );

    trace.setLabel('path', 'live-fallback');
    trace.setLabel('refresh', 'queued');
    const { publicRoutes } = await import('./public');
    return publicRoutes.fetch(c.req.raw, c.env, c.executionCtx);
  }

  const historyByMonitorId = historySnapshot
    ? toPublicAnalyticsOverviewEntryMap(historySnapshot)
    : null;
  const monitorIds = monitors.map((monitor) => monitor.id);
  const runtimeSnapshot =
    monitorIds.length > 0
      ? await trace.timeAsync(
          'runtime_snapshot',
          async () => await readPublicMonitorRuntimeTotalsSnapshot(c.env.DB, rangeEnd),
        )
      : null;
  const runtimeByMonitorId = runtimeSnapshot ? toMonitorRuntimeTotalsEntryMap(runtimeSnapshot) : null;
  const missingRuntimeHistoricalEntry =
    monitors.length > 0 &&
    (!runtimeByMonitorId ||
      monitors.some((monitor) => !runtimeByMonitorId.has(monitor.id) && monitor.created_at < rangeEndFullDays));
  if (monitors.length > 0 && (historySnapshot === null || missingRuntimeHistoricalEntry)) {
    trace.setLabel('path', 'live-fallback');
    const { publicRoutes } = await import('./public');
    return publicRoutes.fetch(c.req.raw, c.env, c.executionCtx);
  }

  let total_sec = 0;
  let downtime_sec = 0;
  let unknown_sec = 0;
  let uptime_sec = 0;

  const partialStart = rangeEndFullDays;
  const partialEnd = rangeEnd;
  const output = monitors.map((monitor) => {
    const historicalTotals = totalsFromAnalyticsOverviewEntry(
      historyByMonitorId?.get(monitor.id),
      range,
    );
    const runtimeEntry = runtimeByMonitorId?.get(monitor.id);
    const partialTotals =
      partialEnd > partialStart && runtimeEntry
        ? materializeMonitorRuntimeTotals(runtimeEntry, partialEnd)
        : { total_sec: 0, downtime_sec: 0, unknown_sec: 0, uptime_sec: 0, uptime_pct: null };

    const totals = {
      total_sec: historicalTotals.total_sec + partialTotals.total_sec,
      downtime_sec: historicalTotals.downtime_sec + partialTotals.downtime_sec,
      unknown_sec: historicalTotals.unknown_sec + partialTotals.unknown_sec,
      uptime_sec: historicalTotals.uptime_sec + partialTotals.uptime_sec,
    };

    total_sec += totals.total_sec;
    downtime_sec += totals.downtime_sec;
    unknown_sec += totals.unknown_sec;
    uptime_sec += totals.uptime_sec;

    return {
      id: monitor.id,
      name: monitor.name,
      type: monitor.type,
      total_sec: totals.total_sec,
      downtime_sec: totals.downtime_sec,
      unknown_sec: totals.unknown_sec,
      uptime_sec: totals.uptime_sec,
      uptime_pct: totals.total_sec === 0 ? 0 : (totals.uptime_sec / totals.total_sec) * 100,
    };
  });

  const res = withVisibilityAwareCaching(
    c.json({
      generated_at: now,
      range,
      range_start_at: rangeStart,
      range_end_at: rangeEnd,
      overall: {
        total_sec,
        downtime_sec,
        unknown_sec,
        uptime_sec,
        uptime_pct: total_sec === 0 ? 0 : (uptime_sec / total_sec) * 100,
      },
      monitors: output,
    }),
    includeHiddenMonitors,
  );
  trace.setLabel('path', 'snapshot');
  trace.finish('total');
  applyTraceToResponse({ res, trace, prefix: 'w' });
  return res;
}

export function registerPublicUiAnalyticsRoutes(app: Hono<{ Bindings: Env }>): void {
  app.get('/analytics/uptime', async (c) => await handlePublicAnalyticsUptime(c));
}

export const publicUiAnalyticsRoutes = new Hono<{ Bindings: Env }>();

publicUiAnalyticsRoutes.use(
  '*',
  cachePublic({
    cacheName: 'uptimer-public',
    maxAgeSeconds: 30,
    normalizeCacheKeyUrl: normalizeAnalyticsUptimeCacheKeyUrl,
  }),
);

registerPublicUiAnalyticsRoutes(publicUiAnalyticsRoutes);
