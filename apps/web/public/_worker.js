const SNAPSHOT_MAX_AGE_SECONDS = 60;
const PREFERRED_MAX_AGE_SECONDS = 30;
const FALLBACK_HTML_MAX_AGE_SECONDS = 600;

function acceptsHtml(request) {
  const accept = request.headers.get('Accept') || '';
  return accept.includes('text/html');
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function safeJsonForInlineScript(value) {
  // Prevent breaking out of <script> tags via `</script>`.
  return JSON.stringify(value).replace(/</g, '\\u003c');
}

function normalizeSnapshotText(value, fallback) {
  if (typeof value !== 'string') return fallback;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
}

function computeCacheControl(ageSeconds) {
  const remaining = Math.max(0, SNAPSHOT_MAX_AGE_SECONDS - ageSeconds);
  const maxAge = Math.min(PREFERRED_MAX_AGE_SECONDS, remaining);
  const stale = Math.max(0, remaining - maxAge);
  return `public, max-age=${maxAge}, stale-while-revalidate=${stale}, stale-if-error=${stale}`;
}

function formatTime(tsSec) {
  try {
    return new Date(tsSec * 1000).toLocaleString();
  } catch {
    return '';
  }
}

function statusDotClass(status) {
  switch (status) {
    case 'up':
      return 'bg-emerald-500 dark:bg-emerald-400';
    case 'down':
      return 'bg-red-500 dark:bg-red-400';
    case 'maintenance':
      return 'bg-blue-500 dark:bg-blue-400';
    case 'paused':
      return 'bg-amber-500 dark:bg-amber-400';
    default:
      return 'bg-slate-400 dark:bg-slate-500';
  }
}

function statusBadgeClass(status) {
  switch (status) {
    case 'up':
      return 'bg-emerald-50 text-emerald-700 ring-emerald-600/20 dark:bg-emerald-500/10 dark:text-emerald-400 dark:ring-emerald-400/20';
    case 'down':
      return 'bg-red-50 text-red-700 ring-red-600/20 dark:bg-red-500/10 dark:text-red-400 dark:ring-red-400/20';
    case 'maintenance':
      return 'bg-blue-50 text-blue-700 ring-blue-600/20 dark:bg-blue-500/10 dark:text-blue-400 dark:ring-blue-400/20';
    case 'paused':
      return 'bg-amber-50 text-amber-700 ring-amber-600/20 dark:bg-amber-500/10 dark:text-amber-400 dark:ring-amber-400/20';
    default:
      return 'bg-slate-50 text-slate-600 ring-slate-500/20 dark:bg-slate-500/10 dark:text-slate-400 dark:ring-slate-400/20';
  }
}

function upsertHeadTag(html, pattern, tag) {
  if (pattern.test(html)) {
    return html.replace(pattern, tag);
  }
  return html.replace('</head>', `  ${tag}\n</head>`);
}

function injectStatusMetaTags(html, snapshot, url) {
  const siteTitle = normalizeSnapshotText(snapshot?.site_title, 'Uptimer');
  const fallbackDescription = normalizeSnapshotText(
    snapshot?.banner?.title,
    'Real-time status and incident updates.',
  );
  const siteDescription = normalizeSnapshotText(snapshot?.site_description, fallbackDescription)
    .replace(/\s+/g, ' ')
    .trim();
  const pageUrl = new URL('/', url).toString();

  const escapedTitle = escapeHtml(siteTitle);
  const escapedDescription = escapeHtml(siteDescription);
  const escapedUrl = escapeHtml(pageUrl);

  let injected = html;
  injected = upsertHeadTag(injected, /<title>[^<]*<\/title>/i, `<title>${escapedTitle}</title>`);
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+name=["']description["'][^>]*>/i,
    `<meta name="description" content="${escapedDescription}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+property=["']og:type["'][^>]*>/i,
    '<meta property="og:type" content="website" />',
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+property=["']og:title["'][^>]*>/i,
    `<meta property="og:title" content="${escapedTitle}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+property=["']og:description["'][^>]*>/i,
    `<meta property="og:description" content="${escapedDescription}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+property=["']og:site_name["'][^>]*>/i,
    `<meta property="og:site_name" content="${escapedTitle}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+property=["']og:url["'][^>]*>/i,
    `<meta property="og:url" content="${escapedUrl}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+name=["']twitter:card["'][^>]*>/i,
    '<meta name="twitter:card" content="summary" />',
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+name=["']twitter:title["'][^>]*>/i,
    `<meta name="twitter:title" content="${escapedTitle}" />`,
  );
  injected = upsertHeadTag(
    injected,
    /<meta[^>]+name=["']twitter:description["'][^>]*>/i,
    `<meta name="twitter:description" content="${escapedDescription}" />`,
  );

  return injected;
}

function monitorGroupLabel(value) {
  if (typeof value !== 'string') return 'Ungrouped';
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : 'Ungrouped';
}

function uptimeBarClass(uptimePct) {
  if (typeof uptimePct !== 'number') {
    return 'bg-slate-300 dark:bg-slate-600';
  }
  if (uptimePct >= 99.95) return 'bg-emerald-500 dark:bg-emerald-400';
  if (uptimePct >= 99) return 'bg-lime-500 dark:bg-lime-400';
  if (uptimePct >= 95) return 'bg-amber-500 dark:bg-amber-400';
  return 'bg-red-500 dark:bg-red-400';
}

function heartbeatBarClass(status) {
  switch (status) {
    case 'up':
      return 'bg-emerald-500 dark:bg-emerald-400';
    case 'down':
      return 'bg-red-500 dark:bg-red-400';
    case 'maintenance':
      return 'bg-blue-500 dark:bg-blue-400';
    default:
      return 'bg-slate-300 dark:bg-slate-600';
  }
}

function renderMiniUptimeBars(days) {
  const items = Array.isArray(days) ? days.slice(-30) : [];
  return items
    .map(
      (day) =>
        `<span class="h-5 flex-1 rounded-sm ${uptimeBarClass(day?.uptime_pct)}" aria-hidden="true"></span>`,
    )
    .join('');
}

function renderMiniHeartbeatBars(heartbeats) {
  const items = Array.isArray(heartbeats) ? heartbeats.slice(0, 30) : [];
  return items
    .map(
      (heartbeat) =>
        `<span class="h-5 flex-1 rounded-sm ${heartbeatBarClass(heartbeat?.status)}" aria-hidden="true"></span>`,
    )
    .join('');
}

function renderIncidentCard(incident) {
  return `
    <div class="ui-panel rounded-xl border border-slate-200/80 dark:border-slate-700/80 bg-white dark:bg-slate-800 p-4">
      <div class="flex items-start justify-between gap-4 mb-2">
        <h4 class="font-semibold text-slate-900 dark:text-slate-100">${escapeHtml(incident?.title ?? 'Incident')}</h4>
        <span class="inline-flex items-center rounded-full font-medium ring-1 ring-inset px-2 py-0.5 text-xs ${statusBadgeClass(
          incident?.impact === 'major' || incident?.impact === 'critical' ? 'down' : 'paused',
        )}">${escapeHtml(incident?.impact ?? 'minor')}</span>
      </div>
      <div class="text-xs text-slate-500 dark:text-slate-400 mb-2">${escapeHtml(formatTime(incident?.started_at ?? 0))}</div>
      ${
        incident?.message
          ? `<p class="text-sm text-slate-600 dark:text-slate-300">${escapeHtml(incident.message)}</p>`
          : ''
      }
    </div>
  `;
}

function renderMaintenanceCard(window, monitorNames) {
  const affected = Array.isArray(window?.monitor_ids)
    ? window.monitor_ids.map((id) => escapeHtml(monitorNames.get(id) || `#${id}`)).join(', ')
    : '';

  return `
    <div class="ui-panel rounded-xl border border-slate-200/80 dark:border-slate-700/80 bg-white dark:bg-slate-800 p-4">
      <div class="flex flex-col gap-2 mb-2">
        <h4 class="font-semibold text-slate-900 dark:text-slate-100">${escapeHtml(window?.title ?? 'Maintenance')}</h4>
        <div class="text-xs text-slate-500 dark:text-slate-400">${escapeHtml(formatTime(window?.starts_at ?? 0))} - ${escapeHtml(formatTime(window?.ends_at ?? 0))}</div>
      </div>
      ${affected ? `<div class="text-sm text-slate-600 dark:text-slate-300 mb-2">Affected: ${affected}</div>` : ''}
      ${window?.message ? `<p class="text-sm text-slate-600 dark:text-slate-300">${escapeHtml(window.message)}</p>` : ''}
    </div>
  `;
}

function renderPreload(snapshot) {
  const overall = typeof snapshot.overall_status === 'string' ? snapshot.overall_status : 'unknown';
  const siteTitle = typeof snapshot.site_title === 'string' ? snapshot.site_title : 'Uptimer';
  const siteDescription =
    typeof snapshot.site_description === 'string' ? snapshot.site_description : '';
  const bannerTitle =
    snapshot && snapshot.banner && typeof snapshot.banner.title === 'string'
      ? snapshot.banner.title
      : 'Status';
  const generatedAt =
    typeof snapshot.generated_at === 'number'
      ? snapshot.generated_at
      : Math.floor(Date.now() / 1000);

  const monitors = Array.isArray(snapshot.monitors) ? snapshot.monitors : [];
  const monitorNames = new Map(
    monitors.map((monitor) => [monitor.id, typeof monitor.name === 'string' ? monitor.name : `#${monitor.id}`]),
  );
  const groups = new Map();
  for (const monitor of monitors) {
    const key = monitorGroupLabel(monitor?.group_name);
    const existing = groups.get(key) || [];
    existing.push(monitor);
    groups.set(key, existing);
  }

  const groupedMonitors = [...groups.entries()]
    .map(
      ([groupName, groupMonitors]) => `
        <div>
          <div class="mb-2 flex items-center justify-between">
            <h4 class="text-sm font-semibold text-slate-600 dark:text-slate-300">${escapeHtml(groupName)}</h4>
            <span class="text-xs text-slate-400 dark:text-slate-500">${groupMonitors.length}</span>
          </div>
          <div class="grid gap-3 sm:grid-cols-2">
            ${groupMonitors
              .map((monitor) => {
                const id = typeof monitor.id === 'number' ? monitor.id : 0;
                const lastCheckedAt =
                  typeof monitor.last_checked_at === 'number' ? monitor.last_checked_at : null;
                const uptimePct =
                  monitor?.uptime_30d && typeof monitor.uptime_30d.uptime_pct === 'number'
                    ? `${monitor.uptime_30d.uptime_pct.toFixed(3)}%`
                    : '-';

                return `
                  <div class="ui-panel rounded-xl border border-slate-200/80 dark:border-slate-700/80 bg-white dark:bg-slate-800 p-4">
                    <div class="mb-3 flex items-start justify-between gap-2">
                      <div class="min-w-0 flex items-center gap-2.5">
                        <span class="relative flex h-2.5 w-2.5"><span class="relative inline-flex h-2.5 w-2.5 rounded-full ${statusDotClass(
                          monitor?.status,
                        )}"></span></span>
                        <div class="min-w-0">
                          <div class="truncate text-base font-semibold text-slate-900 dark:text-slate-100">${escapeHtml(monitor?.name ?? `#${id}`)}</div>
                          <div class="mt-0.5 text-xs text-slate-500 dark:text-slate-400 uppercase tracking-wide">${escapeHtml(monitor?.type ?? '')}</div>
                        </div>
                      </div>
                      <div class="flex items-center gap-2">
                        <span class="text-xs text-slate-400 dark:text-slate-500">${escapeHtml(uptimePct)}</span>
                        <span class="inline-flex items-center rounded-full font-medium ring-1 ring-inset px-2 py-0.5 text-xs ${statusBadgeClass(
                          monitor?.status,
                        )}">${escapeHtml(monitor?.status ?? 'unknown')}</span>
                      </div>
                    </div>

                    <div class="mb-2">
                      <div class="mb-2 text-[11px] text-slate-400 dark:text-slate-500">Availability (30d)</div>
                      <div class="flex h-5 items-end gap-[2px] overflow-hidden">${renderMiniUptimeBars(
                        monitor?.uptime_days,
                      )}</div>
                    </div>

                    <div class="mt-2">
                      <div class="mb-2 text-[11px] text-slate-400 dark:text-slate-500">Recent checks</div>
                      <div class="flex h-5 items-end gap-[2px] overflow-hidden">${renderMiniHeartbeatBars(
                        monitor?.heartbeats,
                      )}</div>
                    </div>

                    <div class="mt-3 text-[11px] text-slate-400 dark:text-slate-500">${lastCheckedAt ? `Last checked: ${escapeHtml(formatTime(lastCheckedAt))}` : 'Never checked'}</div>
                  </div>
                `;
              })
              .join('')}
          </div>
        </div>
      `,
    )
    .join('');

  const activeIncidents = Array.isArray(snapshot.active_incidents) ? snapshot.active_incidents : [];
  const activeMaintenance = Array.isArray(snapshot?.maintenance_windows?.active)
    ? snapshot.maintenance_windows.active
    : [];
  const upcomingMaintenance = Array.isArray(snapshot?.maintenance_windows?.upcoming)
    ? snapshot.maintenance_windows.upcoming
    : [];
  const resolvedIncidentPreview = snapshot?.resolved_incident_preview ?? null;
  const maintenanceHistoryPreview = snapshot?.maintenance_history_preview ?? null;

  return `
    <div class="min-h-screen bg-slate-50 dark:bg-slate-900">
      <header class="sticky top-0 z-20 border-b border-slate-200/70 bg-white/95 backdrop-blur dark:border-slate-700/80 dark:bg-slate-800/95">
        <div class="mx-auto max-w-5xl px-4 py-4 flex justify-between items-center">
          <div class="min-w-0">
            <div class="text-lg font-bold text-slate-900 dark:text-slate-100">${escapeHtml(siteTitle)}</div>
            ${siteDescription ? `<div class="text-sm text-slate-500 dark:text-slate-400 truncate">${escapeHtml(siteDescription)}</div>` : ''}
          </div>
          <span class="inline-flex items-center rounded-full font-medium ring-1 ring-inset px-2.5 py-1 text-sm ${statusBadgeClass(
            overall,
          )}">${escapeHtml(overall)}</span>
        </div>
      </header>

      <main class="max-w-5xl mx-auto px-4 py-6">
        <div class="rounded-2xl p-5 border border-slate-100 dark:border-slate-700 bg-white dark:bg-slate-800 shadow-soft dark:shadow-none mb-6">
          <div class="text-sm text-slate-500 dark:text-slate-300">${escapeHtml(bannerTitle)}</div>
          <div class="text-xs text-slate-400 dark:text-slate-500 mt-1">Updated: ${escapeHtml(formatTime(generatedAt))}</div>
        </div>

        ${
          activeMaintenance.length > 0 || upcomingMaintenance.length > 0
            ? `
            <section class="mb-6">
              <h3 class="text-base font-semibold text-slate-900 dark:text-slate-100 mb-3">Scheduled Maintenance</h3>
              ${activeMaintenance.length > 0 ? `<div class="space-y-3 mb-4">${activeMaintenance.map((window) => renderMaintenanceCard(window, monitorNames)).join('')}</div>` : ''}
              ${upcomingMaintenance.length > 0 ? `<div class="space-y-3">${upcomingMaintenance.map((window) => renderMaintenanceCard(window, monitorNames)).join('')}</div>` : ''}
            </section>
          `
            : ''
        }

        ${
          activeIncidents.length > 0
            ? `
            <section class="mb-6">
              <h3 class="text-base font-semibold text-slate-900 dark:text-slate-100 mb-3">Active Incidents</h3>
              <div class="space-y-3">${activeIncidents.map((incident) => renderIncidentCard(incident)).join('')}</div>
            </section>
          `
            : ''
        }

        <section>
          <h3 class="text-base font-semibold text-slate-900 dark:text-slate-100 mb-3">Services</h3>
          <div class="space-y-5">${groupedMonitors}</div>
        </section>

        <section class="mt-6 pt-6 border-t border-slate-100 dark:border-slate-800 space-y-6">
          <div>
            <h3 class="text-base font-semibold text-slate-900 dark:text-slate-100 mb-3">Incident History</h3>
            ${
              resolvedIncidentPreview
                ? renderIncidentCard(resolvedIncidentPreview)
                : `<div class="ui-panel rounded-xl border border-slate-200/80 dark:border-slate-700/80 bg-white dark:bg-slate-800 p-6 text-center text-slate-500 dark:text-slate-400">No past incidents</div>`
            }
          </div>

          <div>
            <h3 class="text-base font-semibold text-slate-900 dark:text-slate-100 mb-3">Maintenance History</h3>
            ${
              maintenanceHistoryPreview
                ? renderMaintenanceCard(maintenanceHistoryPreview, monitorNames)
                : `<div class="ui-panel rounded-xl border border-slate-200/80 dark:border-slate-700/80 bg-white dark:bg-slate-800 p-6 text-center text-slate-500 dark:text-slate-400">No past maintenance</div>`
            }
          </div>
        </section>
      </main>
    </div>
  `;
}

async function fetchIndexHtml(env, url) {
  const indexUrl = new URL('/index.html', url);

  // Do not pass the original navigation request as init. In Pages runtime the
  // navigation request can carry redirect mode = manual; if we forward that
  // into `env.ASSETS.fetch`, we might accidentally return a redirect response
  // (and cache it), causing ERR_TOO_MANY_REDIRECTS.
  const req = new Request(indexUrl.toString(), {
    method: 'GET',
    headers: { Accept: 'text/html' },
    redirect: 'follow',
  });

  return env.ASSETS.fetch(req);
}

async function fetchPublicHomepageSnapshot(env) {
  const apiOrigin = env.UPTIMER_API_ORIGIN;
  if (typeof apiOrigin !== 'string' || apiOrigin.length === 0) return null;

  const statusUrl = new URL('/api/v1/public/homepage', apiOrigin);

  // Keep HTML fast: if the API is slow, fall back to a static HTML shell.
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), 800);

  try {
    const resp = await fetch(statusUrl.toString(), {
      headers: { Accept: 'application/json' },
      signal: controller.signal,
    });

    if (!resp.ok) return null;

    const data = await resp.json();
    if (!data || typeof data !== 'object') return null;

    return data;
  } catch {
    return null;
  } finally {
    clearTimeout(t);
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // HTML requests: serve SPA entry for client-side routes.
    const wantsHtml = request.method === 'GET' && acceptsHtml(request);

    // Special-case the status page for HTML injection.
    const isStatusPage = url.pathname === '/' || url.pathname === '/index.html';
    if (wantsHtml && isStatusPage) {
      const cacheKey = new Request(url.origin + '/', { method: 'GET' });
      const fallbackCacheKey = new Request(url.origin + '/__uptimer_homepage_fallback__', {
        method: 'GET',
      });
      const cached = await caches.default.match(cacheKey);
      if (cached) return cached;

      const base = await fetchIndexHtml(env, url);
      const html = await base.text();

      const snapshot = await fetchPublicHomepageSnapshot(env);
      if (!snapshot) {
        const fallback = await caches.default.match(fallbackCacheKey);
        if (fallback) {
          return fallback;
        }

        const headers = new Headers(base.headers);
        headers.set('Content-Type', 'text/html; charset=utf-8');
        headers.append('Vary', 'Accept');
        headers.delete('Location');

        return new Response(html, { status: 200, headers });
      }

      const now = Math.floor(Date.now() / 1000);
      const generatedAt = typeof snapshot.generated_at === 'number' ? snapshot.generated_at : now;
      const age = Math.max(0, now - generatedAt);

      let injected = html.replace(
        '<div id="root"></div>',
        `<div id="uptimer-preload">${renderPreload(snapshot)}</div><div id="root"></div>`,
      );

      injected = injectStatusMetaTags(injected, snapshot, url);

      injected = injected.replace(
        '</head>',
        `  <script>globalThis.__UPTIMER_INITIAL_HOMEPAGE__=${safeJsonForInlineScript(snapshot)};</script>\n</head>`,
      );

      const headers = new Headers(base.headers);
      headers.set('Content-Type', 'text/html; charset=utf-8');
      headers.set('Cache-Control', computeCacheControl(age));
      headers.append('Vary', 'Accept');
      headers.delete('Location');

      const resp = new Response(injected, { status: 200, headers });

      const fallbackHeaders = new Headers(headers);
      fallbackHeaders.set('Cache-Control', `public, max-age=${FALLBACK_HTML_MAX_AGE_SECONDS}`);
      const fallbackResp = new Response(injected, { status: 200, headers: fallbackHeaders });

      ctx.waitUntil(
        Promise.all([
          caches.default.put(cacheKey, resp.clone()),
          caches.default.put(fallbackCacheKey, fallbackResp),
        ]),
      );
      return resp;
    }

    // Default: serve static assets.
    const assetResp = await env.ASSETS.fetch(request);

    // SPA fallback for client-side routes.
    if (wantsHtml && assetResp.status === 404) {
      const indexResp = await fetchIndexHtml(env, url);
      const html = await indexResp.text();

      const headers = new Headers(indexResp.headers);
      headers.set('Content-Type', 'text/html; charset=utf-8');
      headers.append('Vary', 'Accept');
      headers.delete('Location');

      return new Response(html, { status: 200, headers });
    }

    return assetResp;
  },
};
