import { describe, expect, it } from 'vitest';

import {
  buildUnknownIntervals,
  mergeIntervals,
  overlapSeconds,
  rangeToSeconds,
  sumIntervals,
  utcDayStart,
} from '../src/analytics/uptime';
import { buildCheckSeries } from './helpers/data-builders';

describe('analytics/uptime', () => {
  it('maps configured ranges to seconds', () => {
    expect(rangeToSeconds('24h')).toBe(86_400);
    expect(rangeToSeconds('7d')).toBe(604_800);
    expect(rangeToSeconds('30d')).toBe(2_592_000);
    expect(rangeToSeconds('90d')).toBe(7_776_000);
    expect(rangeToSeconds('invalid' as unknown as '24h')).toBe('invalid');
  });

  it('normalizes timestamps to UTC day starts', () => {
    expect(utcDayStart(0)).toBe(0);
    expect(utcDayStart(86_399)).toBe(0);
    expect(utcDayStart(86_400)).toBe(86_400);
    expect(utcDayStart(1700000123)).toBe(Math.floor(1700000123 / 86_400) * 86_400);
  });

  it('merges overlapping intervals and sums total seconds', () => {
    expect(mergeIntervals([])).toEqual([]);

    const merged = mergeIntervals([
      { start: 100, end: 120 },
      { start: 80, end: 110 },
      { start: 120, end: 140 },
      { start: 200, end: 220 },
    ]);

    expect(merged).toEqual([
      { start: 80, end: 140 },
      { start: 200, end: 220 },
    ]);
    expect(sumIntervals(merged)).toBe(80);
    expect(sumIntervals([{ start: 10, end: 5 }])).toBe(0);
  });

  it('computes overlap duration across sorted interval lists', () => {
    const a = [
      { start: 0, end: 100 },
      { start: 200, end: 400 },
    ];
    const b = [
      { start: 50, end: 120 },
      { start: 250, end: 260 },
      { start: 300, end: 450 },
    ];

    expect(overlapSeconds(a, b)).toBe(50 + 10 + 100);

    const sparse = [{ start: 0, end: 10 }, undefined] as unknown as Array<{
      start: number;
      end: number;
    }>;
    expect(overlapSeconds(sparse, [{ start: 0, end: 10 }])).toBe(10);
  });

  it('returns empty unknown windows for invalid ranges', () => {
    const checks = buildCheckSeries({
      rangeStart: 0,
      intervalSec: 60,
      points: 3,
      statusAt: () => 'up',
    });
    expect(buildUnknownIntervals(100, 100, 60, checks)).toEqual([]);
  });

  it('marks the full range unknown when interval is invalid or no checks exist', () => {
    expect(buildUnknownIntervals(0, 300, 0, [])).toEqual([{ start: 0, end: 300 }]);
    expect(buildUnknownIntervals(0, 300, 60, [])).toEqual([{ start: 0, end: 300 }]);
  });

  it('marks stale gaps as unknown beyond 2x interval jitter', () => {
    const checks = [
      { checked_at: 0, status: 'up' },
      { checked_at: 300, status: 'up' },
    ];

    const unknown = buildUnknownIntervals(0, 600, 60, checks, 120);
    expect(unknown).toEqual([
      { start: 120, end: 300 },
      { start: 420, end: 600 },
    ]);
  });

  it('uses checks before/after range boundaries when building unknown intervals', () => {
    const checks = [
      { checked_at: -60, status: 'up' },
      { checked_at: 60, status: 'unknown' },
      { checked_at: 120, status: 'up' },
      { checked_at: 300, status: 'up' },
    ];

    expect(buildUnknownIntervals(0, 180, 60, checks)).toEqual([{ start: 60, end: 120 }]);
  });

  it('marks partial stale tails when last check only covers part of a segment', () => {
    const checks = [
      { checked_at: 0, status: 'up' },
      { checked_at: 100, status: 'up' },
    ];

    expect(buildUnknownIntervals(0, 300, 60, checks, 120)).toEqual([{ start: 220, end: 300 }]);
  });

  it('marks windows unknown while the latest known status is unknown', () => {
    const checks = buildCheckSeries({
      rangeStart: 0,
      intervalSec: 60,
      points: 5,
      statusAt: (idx) => (idx < 2 ? 'unknown' : 'up'),
    });

    const unknown = buildUnknownIntervals(0, 300, 60, checks);
    expect(unknown).toEqual([{ start: 0, end: 120 }]);
  });
});
