import { writeFile } from 'node:fs/promises';

import { describe, expect, it } from 'vitest';

import { computePublicHomepagePayload } from '../src/public/homepage';
import { createFakeD1Database, type FakeD1QueryHandler } from './helpers/fake-d1';

type Scenario = {
  name: string;
  monitorCount: number;
  heartbeatPoints: number;
  uptimeDays: number;
};

type Sample = {
  elapsedMs: number;
  monitorCount: number;
  heartbeatRows: number;
  rollupRows: number;
};

const BENCH_LABEL = process.env.HOMEPAGE_BENCH_LABEL ?? 'current-working-tree';
const OUTPUT_PATH = process.env.HOMEPAGE_BENCH_OUTPUT ?? null;

const SCENARIOS: Scenario[] = [
  { name: '1000 monitors / 30 heartbeats / 14 uptime days', monitorCount: 1000, heartbeatPoints: 30, uptimeDays: 14 },
  { name: '5000 monitors / 30 heartbeats / 14 uptime days', monitorCount: 5000, heartbeatPoints: 30, uptimeDays: 14 },
];

function parsePositiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;

  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return fallback;
  }
  return parsed;
}

const WARMUP_RUNS = parsePositiveIntEnv('HOMEPAGE_BENCH_WARMUPS', 3);
const MEASURE_RUNS = parsePositiveIntEnv('HOMEPAGE_BENCH_RUNS', 12);

const scenarioCache = new Map<string, ReturnType<typeof buildScenarioRows>>();

function buildScenarioRows(scenario: Scenario, now: number) {
  const monitors = Array.from({ length: scenario.monitorCount }, (_, index) => ({
    id: index + 1,
    name: `Monitor ${index + 1}`,
    type: 'http',
    group_name: index % 2 === 0 ? 'Core' : 'Edge',
    group_sort_order: index % 2,
    sort_order: index,
    interval_sec: 60,
    created_at: now - 40 * 86_400,
    state_status: 'up',
    last_checked_at: now - 30,
    last_latency_ms: 40 + (index % 50),
  }));

  const heartbeats = monitors.flatMap((monitor) =>
    Array.from({ length: scenario.heartbeatPoints }, (_, index) => ({
      monitor_id: monitor.id,
      checked_at: now - (index + 1) * 60,
      status: 'up',
      latency_ms: 40 + ((monitor.id + index) % 50),
    })),
  );

  const rollups = monitors.flatMap((monitor) =>
    Array.from({ length: scenario.uptimeDays }, (_, index) => ({
      monitor_id: monitor.id,
      day_start_at: now - (scenario.uptimeDays - index) * 86_400,
      total_sec: 86_400,
      downtime_sec: 0,
      unknown_sec: 0,
      uptime_sec: 86_400,
    })),
  );

  return { monitors, heartbeats, rollups };
}

function getScenarioRows(scenario: Scenario, now: number) {
  const key = `${scenario.name}:${now}`;
  const cached = scenarioCache.get(key);
  if (cached) return cached;

  const built = buildScenarioRows(scenario, now);
  scenarioCache.set(key, built);
  return built;
}

function createDbForScenario(scenario: Scenario, now: number) {
  const rows = getScenarioRows(scenario, now);

  const handlers: FakeD1QueryHandler[] = [
    {
      match: 'from monitors m',
      all: () => rows.monitors,
    },
    {
      match: 'select distinct mwm.monitor_id',
      all: () => [],
    },
    {
      match: (sql) => sql.startsWith('select value from settings where key = ?1'),
      first: () => ({ value: '3' }),
    },
    {
      match: 'row_number() over',
      all: () => rows.heartbeats,
    },
    {
      match: 'from monitor_daily_rollups',
      all: () => rows.rollups,
    },
    {
      match: (sql) => sql.startsWith('select key, value from settings'),
      all: () => [
        { key: 'site_title', value: 'Status Hub' },
        { key: 'site_description', value: 'Production services' },
        { key: 'site_locale', value: 'en' },
        { key: 'site_timezone', value: 'UTC' },
      ],
    },
    {
      match: 'from incidents',
      all: () => [],
    },
    {
      match: 'from maintenance_windows',
      all: () => [],
    },
  ];

  return {
    db: createFakeD1Database(handlers),
    rowCounts: {
      monitorCount: rows.monitors.length,
      heartbeatRows: rows.heartbeats.length,
      rollupRows: rows.rollups.length,
    },
  };
}

async function runOne(scenario: Scenario): Promise<Sample> {
  const now = 1_728_000_000;
  const { db, rowCounts } = createDbForScenario(scenario, now);

  const started = performance.now();
  const payload = await computePublicHomepagePayload(db, now);
  const elapsedMs = performance.now() - started;

  expect(payload.monitors).toHaveLength(scenario.monitorCount);

  return {
    elapsedMs,
    ...rowCounts,
  };
}

function percentile(sorted: number[], ratio: number): number {
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * ratio) - 1));
  return sorted[index] ?? 0;
}

function summarize(scenario: Scenario, samples: Sample[]) {
  const elapsed = samples.map((sample) => sample.elapsedMs).sort((a, b) => a - b);
  const totalElapsed = elapsed.reduce((sum, value) => sum + value, 0);
  const first = samples[0];

  return {
    scenario: scenario.name,
    runs: samples.length,
    meanMs: Number((totalElapsed / samples.length).toFixed(3)),
    medianMs: Number(percentile(elapsed, 0.5).toFixed(3)),
    p95Ms: Number(percentile(elapsed, 0.95).toFixed(3)),
    monitorCount: first?.monitorCount ?? 0,
    heartbeatRows: first?.heartbeatRows ?? 0,
    rollupRows: first?.rollupRows ?? 0,
  };
}

describe('homepage snapshot benchmark', () => {
  it('measures homepage snapshot compute cost', async () => {
    const rows = [];

    for (const scenario of SCENARIOS) {
      for (let index = 0; index < WARMUP_RUNS; index += 1) {
        await runOne(scenario);
      }

      const samples: Sample[] = [];
      for (let index = 0; index < MEASURE_RUNS; index += 1) {
        samples.push(await runOne(scenario));
      }

      rows.push(summarize(scenario, samples));
    }

    console.log('Homepage snapshot benchmark');
    console.log(`Label: ${BENCH_LABEL}`);
    if (process.env.HOMEPAGE_BENCH_RUNS || process.env.HOMEPAGE_BENCH_WARMUPS) {
      console.log(
        `Runs: ${process.env.HOMEPAGE_BENCH_RUNS ?? '12'} (warmups: ${process.env.HOMEPAGE_BENCH_WARMUPS ?? '3'})`,
      );
    }
    console.log('');
    console.table(rows);

    if (OUTPUT_PATH) {
      await writeFile(OUTPUT_PATH, JSON.stringify(rows, null, 2), 'utf8');
      console.log(`Wrote raw benchmark data to ${OUTPUT_PATH}`);
    }
  });
});
