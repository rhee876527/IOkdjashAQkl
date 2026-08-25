import type { Env } from '../env';

import { readSettings } from '../settings';
import { acquireLease } from './lock';

const LOCK_NAME = 'retention:check_results';
const LOCK_LEASE_SECONDS = 10 * 60;
const NOTIFICATION_DELIVERIES_RETENTION_DAYS = 90;
const NOTIFICATION_DELIVERIES_MAX_BATCHES = 20;

// Keep delete batches bounded to avoid long-running SQLite statements.
const DELETE_BATCH_SIZE = 5_000;
const MAX_BATCHES = 40; // 200k rows max per run

export async function runRetention(env: Env, controller: ScheduledController): Promise<void> {
  const now = Math.floor((controller.scheduledTime ?? Date.now()) / 1000);

  const acquired = await acquireLease(env.DB, LOCK_NAME, now, LOCK_LEASE_SECONDS);
  if (!acquired) return;

  const settings = await readSettings(env.DB);
  const retentionDays = settings.retention_check_results_days;

  const cutoff = now - retentionDays * 86400;
  if (!Number.isFinite(cutoff) || cutoff <= 0) return;

  let totalDeleted = 0;

  for (let i = 0; i < MAX_BATCHES; i++) {
    const r = await env.DB.prepare(
      `
        DELETE FROM check_results
        WHERE id IN (
          SELECT id
          FROM check_results
          WHERE checked_at < ?1
          ORDER BY checked_at
          LIMIT ?2
        )
      `,
    )
      .bind(cutoff, DELETE_BATCH_SIZE)
      .run();

    const deleted = r.meta.changes ?? 0;
    totalDeleted += deleted;

    if (deleted < DELETE_BATCH_SIZE) break;
  }

  console.log(`retention: deleted=${totalDeleted} cutoff=${cutoff} days=${retentionDays}`);

  await runNotificationDeliveriesRetention(env, now);
  await runExpiredLocksCleanup(env, now);
}

async function runNotificationDeliveriesRetention(env: Env, now: number): Promise<void> {
  const cutoff =
    now - NOTIFICATION_DELIVERIES_RETENTION_DAYS * 86400;
  if (!Number.isFinite(cutoff) || cutoff <= 0) return;

  let totalDeleted = 0;

  for (let i = 0; i < NOTIFICATION_DELIVERIES_MAX_BATCHES; i++) {
    const r = await env.DB.prepare(
      `
        DELETE FROM notification_deliveries
        WHERE id IN (
          SELECT id
          FROM notification_deliveries
          WHERE created_at < ?1
          ORDER BY created_at
          LIMIT ?2
        )
      `,
    )
      .bind(cutoff, DELETE_BATCH_SIZE)
      .run();

    const deleted = r.meta.changes ?? 0;
    totalDeleted += deleted;

    if (deleted < DELETE_BATCH_SIZE) break;
  }

  if (totalDeleted > 0) {
    console.log(`retention: notification_deliveries deleted=${totalDeleted} cutoff=${cutoff}`);
  }
}

async function runExpiredLocksCleanup(env: Env, now: number): Promise<void> {
  try {
    const r = await env.DB.prepare('DELETE FROM locks WHERE expires_at < ?1')
      .bind(now - 60)
      .run();
    const deleted = r.meta.changes ?? 0;
    if (deleted > 0) {
      console.log(`retention: expired locks deleted=${deleted}`);
    }
  } catch (err) {
    console.warn('retention: expired locks cleanup failed', err);
  }
}
