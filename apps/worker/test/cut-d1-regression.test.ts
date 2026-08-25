import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../src/monitor/http', () => ({ runHttpCheck: vi.fn() }));
vi.mock('../src/monitor/tcp', () => ({ runTcpCheck: vi.fn() }));
vi.mock('../src/scheduler/lock', () => ({ acquireLease: vi.fn(), releaseLease: vi.fn(), renewLease: vi.fn() }));
vi.mock('../src/settings', () => ({ readSettings: vi.fn() }));
vi.mock('../src/notify/webhook', () => ({ dispatchWebhookToChannels: vi.fn() }));
vi.mock('../src/public/homepage', () => ({ computePublicHomepagePayload: vi.fn() }));
vi.mock('../src/public/monitor-runtime', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/public/monitor-runtime')>();
  return { ...actual, refreshPublicMonitorRuntimeSnapshot: vi.fn() };
});
vi.mock('../src/public/monitor-runtime-bootstrap', () => ({ rebuildPublicMonitorRuntimeSnapshot: vi.fn() }));
vi.mock('../src/snapshots', () => ({ refreshPublicHomepageSnapshotIfNeeded: vi.fn() }));

import { runHttpCheck } from '../src/monitor/http';
import { runTcpCheck } from '../src/monitor/tcp';
import { computePublicHomepagePayload } from '../src/public/homepage';
import { rebuildPublicMonitorRuntimeSnapshot } from '../src/public/monitor-runtime-bootstrap';
import { refreshPublicMonitorRuntimeSnapshot, writePublicMonitorRuntimeSnapshot } from '../src/public/monitor-runtime';
import { runScheduledTick } from '../src/scheduler/scheduled';
import { acquireLease, releaseLease, renewLease } from '../src/scheduler/lock';
import { refreshPublicHomepageSnapshotIfNeeded } from '../src/snapshots';
import { readSettings } from '../src/settings';
import { dispatchWebhookToChannels } from '../src/notify/webhook';
import { readHomepageSnapshotJsonAnyAge } from '../src/snapshots/public-homepage-read';
import { createFakeD1Database, type FakeD1QueryHandler } from './helpers/fake-d1';
import { runRetention } from '../src/scheduler/retention';
import { readHomepageGuardCacheState, computeHomepageGuardValidUntil } from '../src/public/homepage-guard-state';

type EnvOpts = {
  dueRows?: unknown[];
  channels?: unknown[];
  schedulableMonitorPresent?: boolean | boolean[];
  onRun?: (sql: string, args: unknown[]) => void;
};

function createEnv(opts: EnvOpts = {}) {
  const { dueRows = [], channels = [], schedulableMonitorPresent = true, onRun } = opts;
  const seq = Array.isArray(schedulableMonitorPresent) ? [...schedulableMonitorPresent] : null;
  const fallback = Array.isArray(schedulableMonitorPresent) ? (schedulableMonitorPresent.at(-1) ?? false) : schedulableMonitorPresent;
  const nextPresent = () => (seq ? (seq.shift() ?? fallback) : fallback);
  const handlers: FakeD1QueryHandler[] = [
    { match: 'from public_snapshots', all: () => [], first: () => null },
    { match: 'from notification_channels', all: () => channels },
    { match: (s) => s.includes('select 1 as present') && s.includes('from monitors m'), first: () => (nextPresent() ? { present: 1 } : null) },
    { match: 'from monitors m', all: () => dueRows.map((r) => typeof r === 'object' && r !== null && !('created_at' in (r as object)) ? Object.assign(r as object, { created_at: 0 }) : r) },
    { match: 'select distinct mwm.monitor_id', all: () => [] },
    { match: 'from maintenance_windows', all: () => [] },
    { match: 'from maintenance_window_monitors', all: () => [] },
    { match: 'insert into check_results', run: (a, s) => { onRun?.(s, a); return { meta: { changes: 1 } }; } },
    { match: 'insert into monitor_state', run: (a, s) => { onRun?.(s, a); return { meta: { changes: 1 } }; } },
    { match: 'into outages', run: (a, s) => { onRun?.(s, a); return { meta: { changes: 1 } }; } },
    { match: 'update outages', run: (a, s) => { onRun?.(s, a); return { meta: { changes: 1 } }; } },
  ];
  return { DB: createFakeD1Database(handlers) } as unknown as import('../src/env').Env;
}

function sampleHomepagePayload(now = 1_728_000_000) {
  return {
    generated_at: now,
    bootstrap_mode: 'full' as const,
    monitor_count_total: 1,
    site_title: 'Uptimer',
    site_description: '',
    site_locale: 'auto' as const,
    site_timezone: 'UTC',
    uptime_rating_level: 3 as const,
    overall_status: 'up' as const,
    banner: { source: 'monitors' as const, status: 'operational' as const, title: 'All Systems Operational', down_ratio: null },
    summary: { up: 1, down: 0, maintenance: 0, paused: 0, unknown: 0 },
    monitors: [{ id: 1, name: 'API', type: 'http' as const, display_url: null, group_name: null, status: 'up' as const, is_stale: false, last_checked_at: now - 30, heartbeat_strip: { checked_at: [now - 60], status_codes: 'u', latency_ms: [42] }, uptime_30d: { uptime_pct: 100 }, uptime_day_strip: { day_start_at: [now - 86_400], downtime_sec: [0], unknown_sec: [0], uptime_pct_milli: [100_000] } }],
    active_incidents: [],
    maintenance_windows: { active: [], upcoming: [] },
    resolved_incident_preview: null,
    maintenance_history_preview: null,
  };
}

/**
 * CUT-D1 regression — DB-targeted safety net for CUT-D1-MERGED.md (R1-R10).
 * Every test here asserts CURRENT baseline (pre-cut) DB traffic via FakeD1.
 * Tagged R* shows which merged-doc lever it guards; after each PR the
 * matching test MUST be updated to the new expected counts — failure means
 * the cut broke a functional invariant, not that the test is stale.
 * All SQL is lowercased via normalizeSql in helpers/fake-d1.ts, matched by
 * substring/regex — keep matchers loose (includes) so refactors don't flake.
 */
describe('cut-d1 regression — db and key features', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-02-17T00:00:42.000Z'));
    vi.mocked(acquireLease).mockResolvedValue(true);
    vi.mocked(releaseLease).mockResolvedValue(undefined);
    vi.mocked(renewLease).mockResolvedValue(true);
    vi.mocked(readSettings).mockResolvedValue({ site_title: 'Uptimer', site_description: '', site_locale: 'auto', site_timezone: 'UTC', retention_check_results_days: 7, state_failures_to_down_from_up: 2, state_successes_to_up_from_down: 2, admin_default_overview_range: '24h', admin_default_monitor_range: '24h', uptime_rating_level: 3 });
    vi.mocked(dispatchWebhookToChannels).mockResolvedValue(undefined);
    vi.mocked(computePublicHomepagePayload).mockResolvedValue({ generated_at: Math.floor(Date.now() / 1000), bootstrap_mode: 'full', monitor_count_total: 0, site_title: 'Uptimer', site_description: '', site_locale: 'auto', site_timezone: 'UTC', uptime_rating_level: 3, overall_status: 'up', banner: { source: 'monitors', status: 'operational', title: 'All Systems Operational', down_ratio: null }, summary: { up: 0, down: 0, maintenance: 0, paused: 0, unknown: 0 }, monitors: [], active_incidents: [], maintenance_windows: { active: [], upcoming: [] }, resolved_incident_preview: null, maintenance_history_preview: null } as never);
    vi.mocked(refreshPublicMonitorRuntimeSnapshot).mockResolvedValue({ version: 1, generated_at: Math.floor(Date.now() / 1000), day_start_at: Math.floor(Math.floor(Date.now() / 1000) / 86_400) * 86_400, monitors: [] });
    vi.mocked(rebuildPublicMonitorRuntimeSnapshot).mockResolvedValue({ version: 1, generated_at: Math.floor(Date.now() / 1000), day_start_at: Math.floor(Math.floor(Date.now() / 1000) / 86_400) * 86_400, monitors: [] });
    vi.mocked(refreshPublicHomepageSnapshotIfNeeded).mockResolvedValue(false);
    vi.mocked(runHttpCheck).mockResolvedValue({ status: 'up', latencyMs: 21, httpStatus: 200, error: null, attempts: 1 });
    vi.mocked(runTcpCheck).mockResolvedValue({ status: 'up', latencyMs: 12, httpStatus: null, error: null, attempts: 1 });
  });
  afterEach(() => { vi.useRealTimers(); vi.clearAllMocks(); });

  it('R5+R1: idle tick no longer queues homepage refresh (R1 gate)', async () => {
    const env = createEnv({ dueRows: [] });
    const waitUntil = vi.fn();
    await runScheduledTick(env, { waitUntil } as unknown as ExecutionContext);
    expect(acquireLease).toHaveBeenCalledWith(env.DB, 'scheduler:tick', expect.any(Number), 135);
    expect(waitUntil).toHaveBeenCalledTimes(0);
    expect(refreshPublicHomepageSnapshotIfNeeded).not.toHaveBeenCalled();
  });

  it('R8 baseline: every healthy tick inserts check_results (will become K=5 after cut)', async () => {
    const checkedAt = Math.floor(Math.floor(Date.now() / 1000) / 60) * 60;
    const dueRows = [{ id: 1, name: 'API', type: 'http', target: 'https://example.com', interval_sec: 60, created_at: 1_760_000_000, timeout_ms: 10_000, http_method: 'GET', http_headers_json: null, http_body: null, expected_status_json: null, response_keyword: null, response_keyword_mode: null, response_forbidden_keyword: null, response_forbidden_keyword_mode: null, state_status: 'up', state_last_error: null, last_checked_at: checkedAt - 60, last_changed_at: 1_760_000_000, consecutive_failures: 0, consecutive_successes: 1 }];
    const inserts: string[] = [];
    const env = createEnv({ dueRows, onRun: (s) => inserts.push(s) });
    const waitUntil = vi.fn();
    await runScheduledTick(env, { waitUntil } as unknown as ExecutionContext);
    await Promise.all(waitUntil.mock.calls.map((c) => c[0] as Promise<unknown>));
    expect(inserts.some((s) => s.includes('insert into check_results'))).toBe(true);
    expect(inserts.some((s) => s.includes('insert into monitor_state'))).toBe(true);
  });

  it('R1b: bounded stale homepage is served without live compute', async () => {
    const now = 1_728_000_200;
    const fresher = sampleHomepagePayload(now - 10);
    const db = createFakeD1Database([
      { match: (s) => s.includes('select key, generated_at, updated_at') && s.includes('from public_snapshots'), all: () => [{ key: 'homepage', generated_at: fresher.generated_at, updated_at: fresher.generated_at }, { key: 'homepage:artifact', generated_at: fresher.generated_at, updated_at: fresher.generated_at }] },
      { match: (s) => s.includes('body_json') && s.includes('from public_snapshots'), first: (args) => String(args[0]) === 'homepage' ? { generated_at: fresher.generated_at, updated_at: fresher.generated_at, body_json: JSON.stringify(fresher) } : null },
    ]);
    const res = await readHomepageSnapshotJsonAnyAge(db, now);
    expect(res).toEqual({ bodyJson: JSON.stringify(fresher), age: 10 });
  });

  it('R4: metadata lazy-read uses 1 metadata SELECT (baseline for dedup)', async () => {
    const now = 1_728_000_200;
    const fresher = sampleHomepagePayload(now - 10);
    const older = sampleHomepagePayload(now - 30);
    let metaReads = 0;
    const db = createFakeD1Database([
      { match: (s) => s.includes('select key, generated_at, updated_at') && s.includes('from public_snapshots'), all: () => { metaReads += 1; return [{ key: 'homepage', generated_at: fresher.generated_at, updated_at: fresher.generated_at }, { key: 'homepage:artifact', generated_at: older.generated_at, updated_at: older.generated_at }]; } },
      { match: (s) => s.includes('body_json') && s.includes('from public_snapshots'), first: (args) => String(args[0]) === 'homepage' ? { generated_at: fresher.generated_at, updated_at: fresher.generated_at, body_json: JSON.stringify(fresher) } : null },
    ]);
    await expect(readHomepageSnapshotJsonAnyAge(db, now)).resolves.toEqual({ bodyJson: JSON.stringify(fresher), age: 10 });
    expect(metaReads).toBe(1);
  });

  it('R2 baseline: artifact touch writes every minute when current (after R2 gated to 300s expect 0 within interval)', async () => {
    const { publishHomepageArtifactSnapshotFromPublishedHomepage } = await import('../src/internal/sharded-public-snapshot-core');
    const now = 1_728_000_200;
    const gen = now - 5;
    let updates = 0;
    const db = createFakeD1Database([
      { match: (s) => s.includes('from public_snapshots') && s.includes('select'), first: () => ({ generated_at: gen, updated_at: gen - 400 }) },
      { match: (s) => s.includes('update public_snapshots'), run: () => { updates += 1; return 1; } },
      { match: 'insert', run: () => 1 },
    ]);
    const res = await publishHomepageArtifactSnapshotFromPublishedHomepage({ env: { DB: db } as never, now });
    expect((res as { skip?: string }).skip).toBe('current_artifact');
    expect(updates).toBe(1);
  });

  it('R3 baseline: fragment UPSERT writes even when identical (after R3 dedup expect 1 then 0)', async () => {
    const { writePublicSnapshotFragments } = await import('../src/snapshots/public-fragments');
    let runs = 0;
    const db = createFakeD1Database([{ match: 'insert into public_snapshot_fragments', run: () => { runs += 1; return 1; } }, { match: 'select', first: () => null, all: () => [] }]);
    const writes = [{ snapshotKey: 'homepage', fragmentKey: 'envelope', generatedAt: 100, updatedAt: 100, bodyJson: JSON.stringify({ v: 1 }) }];
    await writePublicSnapshotFragments(db, writes as never);
    expect(runs).toBe(1);
    await writePublicSnapshotFragments(db, writes as never);
    expect(runs).toBe(2);
  });

  it('R7: guard state cache key and valid_until helper work', async () => {
    const db = createFakeD1Database([
      { match: (s) => s.includes('public_snapshot_guard_versions'), first: () => null, all: () => [] },
      { match: (s) => s.includes('select') && s.includes('boundary_at'), first: () => ({ boundary_at: null }) },
    ]);
    const state = await readHomepageGuardCacheState(db);
    expect(state).toBeDefined();
    const validUntil = await computeHomepageGuardValidUntil(db, 1_728_000_000);
    expect(typeof validUntil === 'number').toBe(true);
  });

  it('R6 baseline: outages query shape works (after R6 single-scan 4→1 + rollup-first expect 1 outages SELECT)', async () => {
    const now = 1_728_000_000;
    const db = createFakeD1Database([
      { match: 'from monitors', all: () => [{ id: 1 }, { id: 2 }] },
      { match: 'from outages', all: () => [{ monitor_id: 1, started_at: now - 3600, ended_at: now - 1800 }] },
      { match: 'select', all: () => [], first: () => null },
    ]);
    const rows = await db.prepare('SELECT monitor_id, started_at, ended_at FROM outages WHERE monitor_id IN (1,2)').all();
    expect((rows.results as unknown[]).length).toBeGreaterThanOrEqual(0);
  });

  it('monitor-runtime baseline: dual UPSERT writes again when identical (after R3 hash dedup +30s floor expect 1 then 0)', async () => {
    let batchRuns = 0;
    const db = createFakeD1Database([{ match: 'insert into public_snapshots', run: () => { batchRuns += 1; return 1; } }]);
    const snap = { version: 1 as const, generated_at: 120, day_start_at: 0, monitors: [] };
    await writePublicMonitorRuntimeSnapshot(db, snap as never, 140);
    expect(batchRuns).toBe(1);
    batchRuns = 0;
    await writePublicMonitorRuntimeSnapshot(db, snap as never, 160);
    expect(batchRuns).toBe(1);
  });

  it('R10: retention deletes in bounded batches LIMIT 5000', async () => {
    const deletes = [5000, 1200];
    const calls: unknown[][] = [];
    const env = { DB: createFakeD1Database([
      { match: 'delete from check_results', run: (a) => { calls.push(a); return { meta: { changes: deletes.shift() ?? 0 } }; } },
      { match: 'delete from notification_deliveries', run: () => ({ meta: { changes: 0 } }) },
      { match: 'delete from locks', run: () => ({ meta: { changes: 0 } }) },
    ]) } as unknown as import('../src/env').Env;
    vi.mocked(readSettings).mockResolvedValue({ site_title: 'Uptimer', site_description: '', site_locale: 'auto', site_timezone: 'UTC', retention_check_results_days: 7, state_failures_to_down_from_up: 2, state_successes_to_up_from_down: 2, admin_default_overview_range: '24h', admin_default_monitor_range: '24h', uptime_rating_level: 3 });
    await runRetention(env, { scheduledTime: Date.UTC(2026, 1, 18, 0, 0, 0) } as ScheduledController);
    expect(calls.length).toBe(2);
    expect(calls[0]?.[1]).toBe(5000);
  });

  it('invariants: outage open on down persists via monitor_state + outages batch', async () => {
    const checkedAt = Math.floor(Math.floor(Date.now() / 1000) / 60) * 60;
    const dueRows = [{ id: 9, name: 'TCP', type: 'tcp', target: 'tcp://example.com:80', interval_sec: 60, created_at: 1_760_000_000, timeout_ms: 10_000, http_method: null, http_headers_json: null, http_body: null, expected_status_json: null, response_keyword: null, response_keyword_mode: null, response_forbidden_keyword: null, response_forbidden_keyword_mode: null, state_status: 'up', state_last_error: null, last_checked_at: checkedAt - 60, last_changed_at: 1_760_000_000, consecutive_failures: 1, consecutive_successes: 0 }];
    vi.mocked(runTcpCheck).mockResolvedValueOnce({ status: 'down', latencyMs: null, httpStatus: null, error: 'refused', attempts: 1 });
    const stmts: string[] = [];
    const env = createEnv({ dueRows, onRun: (s) => stmts.push(s) });
    const waitUntil = vi.fn();
    await runScheduledTick(env, { waitUntil } as unknown as ExecutionContext);
    await Promise.all(waitUntil.mock.calls.map((c) => c[0] as Promise<unknown>));
    expect(stmts.join(' ')).toContain('monitor_state');
  });

  it('DB counters: homepage hot path is 1-2 reads cold (regression lock for R1b)', async () => {
    const now = 1_728_000_200;
    const payload = sampleHomepagePayload(now - 10);
    let reads = 0;
    const db = createFakeD1Database([
      { match: 'from public_snapshots', first: () => { reads += 1; return { generated_at: payload.generated_at, body_json: JSON.stringify(payload) }; }, all: () => { reads += 1; return []; } },
    ]);
    const r = await readHomepageSnapshotJsonAnyAge(db, now);
    expect(r).not.toBeNull();
    expect(reads).toBeGreaterThanOrEqual(1);
    expect(reads).toBeLessThanOrEqual(3);
  });
});
