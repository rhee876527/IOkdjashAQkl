import { Hono } from 'hono';

import type { Env } from '../env';
import { AppError } from '../middleware/errors';
import { computePublicHomepagePayload } from '../public/homepage';
import { refreshPublicHomepageSnapshotIfNeeded } from '../snapshots';
import { parseSettingsPatch, patchSettings, readSettings } from '../settings';

export const adminSettingsRoutes = new Hono<{ Bindings: Env }>();

function queuePublicHomepageSnapshotRefresh(c: { env: Env; executionCtx: ExecutionContext }) {
  const now = Math.floor(Date.now() / 1000);
  c.executionCtx.waitUntil(
    refreshPublicHomepageSnapshotIfNeeded({
      db: c.env.DB,
      now,
      compute: () => computePublicHomepagePayload(c.env.DB, Math.floor(Date.now() / 1000)),
    }).catch((err) => {
      console.warn('homepage snapshot: refresh failed', err);
    }),
  );
}

adminSettingsRoutes.get('/', async (c) => {
  const settings = await readSettings(c.env.DB);
  return c.json({ settings });
});

adminSettingsRoutes.patch('/', async (c) => {
  const rawBody = await c.req.json().catch(() => {
    throw new AppError(400, 'INVALID_ARGUMENT', 'Invalid JSON body');
  });

  const patch = parseSettingsPatch(rawBody);
  await patchSettings(c.env.DB, patch);

  queuePublicHomepageSnapshotRefresh(c);

  const settings = await readSettings(c.env.DB);
  return c.json({ settings });
});
