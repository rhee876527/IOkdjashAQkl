import type { PublicHomepageResponse } from '../schemas/public-homepage';

import {
  buildPublicMonitorCards,
  buildPublicStatusBanner,
  listIncidentMonitorIdsByIncidentId,
  listMaintenanceWindowMonitorIdsByWindowId,
  listVisibleActiveIncidents,
  listVisibleMaintenanceWindows,
  readPublicSiteSettings,
  toIncidentImpact,
  toIncidentStatus,
  type IncidentRow,
  type MaintenanceWindowRow,
} from './data';
import {
  filterStatusPageScopedMonitorIds,
  incidentStatusPageVisibilityPredicate,
  listStatusPageVisibleMonitorIds,
  maintenanceWindowStatusPageVisibilityPredicate,
  shouldIncludeStatusPageScopedItem,
} from './visibility';

const PREVIEW_BATCH_LIMIT = 50;

type IncidentSummary = PublicHomepageResponse['active_incidents'][number];
type MaintenancePreview = NonNullable<PublicHomepageResponse['maintenance_history_preview']>;

function toIncidentSummary(row: IncidentRow): IncidentSummary {
  return {
    id: row.id,
    title: row.title,
    status: toIncidentStatus(row.status),
    impact: toIncidentImpact(row.impact),
    message: row.message,
    started_at: row.started_at,
    resolved_at: row.resolved_at,
  };
}

function toMaintenancePreview(
  row: MaintenanceWindowRow,
  monitorIds: number[],
): MaintenancePreview {
  return {
    id: row.id,
    title: row.title,
    message: row.message,
    starts_at: row.starts_at,
    ends_at: row.ends_at,
    monitor_ids: monitorIds,
  };
}

function toHomepageMonitorCard(
  monitor: Awaited<ReturnType<typeof buildPublicMonitorCards>>['monitors'][number],
): PublicHomepageResponse['monitors'][number] {
  return {
    id: monitor.id,
    name: monitor.name,
    type: monitor.type,
    group_name: monitor.group_name,
    status: monitor.status,
    is_stale: monitor.is_stale,
    last_checked_at: monitor.last_checked_at,
    heartbeats: monitor.heartbeats,
    uptime_30d: monitor.uptime_30d
      ? {
          uptime_pct: monitor.uptime_30d.uptime_pct,
        }
      : null,
    uptime_days: monitor.uptime_days.map((day) => ({
      day_start_at: day.day_start_at,
      downtime_sec: day.downtime_sec,
      unknown_sec: day.unknown_sec,
      uptime_pct: day.uptime_pct,
    })),
  };
}

async function findLatestVisibleResolvedIncident(
  db: D1Database,
  includeHiddenMonitors: boolean,
): Promise<IncidentRow | null> {
  const incidentVisibilitySql = incidentStatusPageVisibilityPredicate(includeHiddenMonitors);
  let cursor: number | null = null;

  while (true) {
    const queryResult: { results: IncidentRow[] | undefined } = cursor
      ? await db
          .prepare(
            `
            SELECT id, title, status, impact, message, started_at, resolved_at
            FROM incidents
            WHERE status = 'resolved'
              AND ${incidentVisibilitySql}
              AND id < ?2
            ORDER BY id DESC
            LIMIT ?1
          `,
          )
          .bind(PREVIEW_BATCH_LIMIT, cursor)
          .all<IncidentRow>()
      : await db
          .prepare(
            `
            SELECT id, title, status, impact, message, started_at, resolved_at
            FROM incidents
            WHERE status = 'resolved'
              AND ${incidentVisibilitySql}
            ORDER BY id DESC
            LIMIT ?1
          `,
          )
          .bind(PREVIEW_BATCH_LIMIT)
          .all<IncidentRow>();

    const rows: IncidentRow[] = queryResult.results ?? [];
    if (rows.length === 0) return null;

    const monitorIdsByIncidentId = await listIncidentMonitorIdsByIncidentId(
      db,
      rows.map((row) => row.id),
    );
    const visibleMonitorIds = includeHiddenMonitors
      ? new Set<number>()
      : await listStatusPageVisibleMonitorIds(
          db,
          [...monitorIdsByIncidentId.values()].flat(),
        );

    for (const row of rows) {
      const originalMonitorIds = monitorIdsByIncidentId.get(row.id) ?? [];
      const filteredMonitorIds = filterStatusPageScopedMonitorIds(
        originalMonitorIds,
        visibleMonitorIds,
        includeHiddenMonitors,
      );

      if (shouldIncludeStatusPageScopedItem(originalMonitorIds, filteredMonitorIds)) {
        return row;
      }
    }

    if (rows.length < PREVIEW_BATCH_LIMIT) {
      return null;
    }

    cursor = rows[rows.length - 1]?.id ?? null;
  }
}

async function findLatestVisibleHistoricalMaintenanceWindow(
  db: D1Database,
  now: number,
  includeHiddenMonitors: boolean,
): Promise<{ row: MaintenanceWindowRow; monitorIds: number[] } | null> {
  const maintenanceVisibilitySql = maintenanceWindowStatusPageVisibilityPredicate(
    includeHiddenMonitors,
  );
  let cursor: number | null = null;

  while (true) {
    const queryResult: { results: MaintenanceWindowRow[] | undefined } = cursor
      ? await db
          .prepare(
            `
            SELECT id, title, message, starts_at, ends_at, created_at
            FROM maintenance_windows
            WHERE ends_at <= ?1
              AND ${maintenanceVisibilitySql}
              AND id < ?3
            ORDER BY id DESC
            LIMIT ?2
          `,
          )
          .bind(now, PREVIEW_BATCH_LIMIT, cursor)
          .all<MaintenanceWindowRow>()
      : await db
          .prepare(
            `
            SELECT id, title, message, starts_at, ends_at, created_at
            FROM maintenance_windows
            WHERE ends_at <= ?1
              AND ${maintenanceVisibilitySql}
            ORDER BY id DESC
            LIMIT ?2
          `,
          )
          .bind(now, PREVIEW_BATCH_LIMIT)
          .all<MaintenanceWindowRow>();

    const rows: MaintenanceWindowRow[] = queryResult.results ?? [];
    if (rows.length === 0) return null;

    const monitorIdsByWindowId = await listMaintenanceWindowMonitorIdsByWindowId(
      db,
      rows.map((row) => row.id),
    );
    const visibleMonitorIds = includeHiddenMonitors
      ? new Set<number>()
      : await listStatusPageVisibleMonitorIds(
          db,
          [...monitorIdsByWindowId.values()].flat(),
        );

    for (const row of rows) {
      const originalMonitorIds = monitorIdsByWindowId.get(row.id) ?? [];
      const filteredMonitorIds = filterStatusPageScopedMonitorIds(
        originalMonitorIds,
        visibleMonitorIds,
        includeHiddenMonitors,
      );

      if (shouldIncludeStatusPageScopedItem(originalMonitorIds, filteredMonitorIds)) {
        return { row, monitorIds: filteredMonitorIds };
      }
    }

    if (rows.length < PREVIEW_BATCH_LIMIT) {
      return null;
    }

    cursor = rows[rows.length - 1]?.id ?? null;
  }
}

export async function computePublicHomepagePayload(
  db: D1Database,
  now: number,
): Promise<PublicHomepageResponse> {
  const includeHiddenMonitors = false;

  const [
    monitorData,
    activeIncidents,
    maintenanceWindows,
    settings,
    resolvedIncidentPreview,
    maintenanceHistoryPreview,
  ] = await Promise.all([
    buildPublicMonitorCards(db, now, { includeHiddenMonitors }),
    listVisibleActiveIncidents(db, includeHiddenMonitors),
    listVisibleMaintenanceWindows(db, now, includeHiddenMonitors),
    readPublicSiteSettings(db),
    findLatestVisibleResolvedIncident(db, includeHiddenMonitors),
    findLatestVisibleHistoricalMaintenanceWindow(db, now, includeHiddenMonitors),
  ]);

  return {
    generated_at: now,
    site_title: settings.site_title,
    site_description: settings.site_description,
    site_locale: settings.site_locale,
    site_timezone: settings.site_timezone,
    uptime_rating_level: monitorData.uptimeRatingLevel,
    overall_status: monitorData.overallStatus,
    banner: buildPublicStatusBanner({
      counts: monitorData.summary,
      monitors: monitorData.monitors,
      activeIncidents,
      activeMaintenanceWindows: maintenanceWindows.active,
    }),
    summary: monitorData.summary,
    monitors: monitorData.monitors.map(toHomepageMonitorCard),
    active_incidents: activeIncidents.map(({ row }) => toIncidentSummary(row)),
    maintenance_windows: {
      active: maintenanceWindows.active.map(({ row, monitorIds }) =>
        toMaintenancePreview(row, monitorIds),
      ),
      upcoming: maintenanceWindows.upcoming.map(({ row, monitorIds }) =>
        toMaintenancePreview(row, monitorIds),
      ),
    },
    resolved_incident_preview: resolvedIncidentPreview
      ? toIncidentSummary(resolvedIncidentPreview)
      : null,
    maintenance_history_preview: maintenanceHistoryPreview
      ? toMaintenancePreview(maintenanceHistoryPreview.row, maintenanceHistoryPreview.monitorIds)
      : null,
  };
}
