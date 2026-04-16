import { AppError } from '../middleware/errors';
import {
  publicHomepageResponseSchema,
  publicHomepageStoredRenderArtifactSchema,
  type PublicHomepageResponse,
} from '../schemas/public-homepage';

const SNAPSHOT_KEY = 'homepage';
const SNAPSHOT_ARTIFACT_KEY = 'homepage:artifact';
const MAX_AGE_SECONDS = 60;
const MAX_STALE_SECONDS = 10 * 60;
const MAX_FUTURE_SNAPSHOT_SKEW_SECONDS = 60;
const SPLIT_SNAPSHOT_VERSION = 3;
const LEGACY_COMBINED_SNAPSHOT_VERSION = 2;
type SnapshotKey = typeof SNAPSHOT_KEY | typeof SNAPSHOT_ARTIFACT_KEY;

const READ_REFRESH_SNAPSHOT_METADATA_SQL = `
  SELECT key, generated_at, updated_at
  FROM public_snapshots
  WHERE key = ?1 OR key = ?2
`;
const READ_REFRESH_SNAPSHOT_ROWS_SQL = `
  SELECT key, generated_at, updated_at, body_json
  FROM public_snapshots
  WHERE key = ?1 OR key = ?2
`;
const READ_REFRESH_SNAPSHOT_ROW_BY_KEY_SQL = `
  SELECT generated_at, updated_at, body_json
  FROM public_snapshots
  WHERE key = ?1
`;
const readRefreshSnapshotMetadataStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const readRefreshSnapshotRowsStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const readRefreshSnapshotRowByKeyStatementByDb = new WeakMap<D1Database, D1PreparedStatement>();
const normalizedHomepagePayloadCacheByDb = new WeakMap<
  D1Database,
  Map<SnapshotKey, NormalizedSnapshotRow>
>();
const normalizedHomepageArtifactCacheByDb = new WeakMap<
  D1Database,
  Map<SnapshotKey, NormalizedSnapshotRow>
>();
const normalizedHomepagePayloadCacheGlobal = new Map<SnapshotKey, RawNormalizedSnapshotRow>();
const normalizedHomepageArtifactCacheGlobal = new Map<SnapshotKey, RawNormalizedSnapshotRow>();
const parsedHomepagePayloadCacheByDb = new WeakMap<
  D1Database,
  Map<SnapshotKey, ParsedSnapshotRow>
>();
const parsedHomepagePayloadCacheGlobal = new Map<SnapshotKey, RawParsedSnapshotRow>();

type SnapshotRefreshRow = {
  key: SnapshotKey;
  generated_at: number;
  body_json: string;
  updated_at?: number | null;
};

type SnapshotRefreshMetadataRow = Pick<SnapshotRefreshRow, 'key' | 'generated_at' | 'updated_at'>;

type SnapshotCandidate = {
  key: SnapshotKey;
  generatedAt: number;
  updatedAt: number;
};

type NormalizedSnapshotRow = {
  generatedAt: number;
  updatedAt: number;
  rawBodyJson: string;
  bodyJson: string;
};

type RawNormalizedSnapshotRow = NormalizedSnapshotRow & {
  rawBodyJson: string;
};

type ParsedSnapshotRow = {
  generatedAt: number;
  updatedAt: number;
  rawBodyJson: string;
  snapshot: PublicHomepageResponse;
};

type RawParsedSnapshotRow = ParsedSnapshotRow & {
  rawBodyJson: string;
};

type ParsedJsonText = {
  trimmed: string;
  value: unknown;
};

type CandidateReadResult = {
  row: NormalizedSnapshotRow | null;
  invalid: boolean;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function parseJsonText(text: string): ParsedJsonText | null {
  const trimmed = text.trim();
  if (!trimmed) return null;
  try {
    return {
      trimmed,
      value: JSON.parse(trimmed) as unknown,
    };
  } catch {
    return null;
  }
}

function normalizeDirectHomepagePayload(
  value: unknown,
): string | null {
  const parsedPayload = parseDirectHomepagePayload(value);
  return parsedPayload ? JSON.stringify(parsedPayload) : null;
}

function parseDirectHomepagePayload(value: unknown): PublicHomepageResponse | null {
  const directPayload = publicHomepageResponseSchema.safeParse(value);
  if (directPayload.success) {
    return directPayload.data;
  }
  if (!isRecord(value)) {
    return null;
  }

  const normalizedPayload = publicHomepageResponseSchema.safeParse({
    ...value,
    bootstrap_mode:
      value.bootstrap_mode === 'full' || value.bootstrap_mode === 'partial'
        ? value.bootstrap_mode
        : 'full',
    monitor_count_total: Array.isArray(value.monitors) ? value.monitors.length : 0,
  });
  return normalizedPayload.success ? normalizedPayload.data : null;
}

function parseStoredHomepageRenderArtifactSnapshot(value: unknown): PublicHomepageResponse | null {
  const artifact = publicHomepageStoredRenderArtifactSchema.safeParse(value);
  if (!artifact.success) {
    return null;
  }

  if ('snapshot' in artifact.data) {
    return artifact.data.snapshot;
  }

  const parsedSnapshot = parseJsonText(artifact.data.snapshot_json);
  if (parsedSnapshot === null) {
    return null;
  }

  return parseDirectHomepagePayload(parsedSnapshot.value);
}

function normalizeHomepagePayloadBodyJsonForKey(
  key: SnapshotKey,
  bodyJson: string,
): string | null {
  const parsed = parseJsonText(bodyJson);
  if (parsed === null) return null;

  if (!isRecord(parsed.value)) {
    return null;
  }

  const version = parsed.value.version;
  if (version === SPLIT_SNAPSHOT_VERSION || version === LEGACY_COMBINED_SNAPSHOT_VERSION) {
    return normalizeDirectHomepagePayload(parsed.value.data);
  }

  if (key === SNAPSHOT_KEY) {
    const directPayload = normalizeDirectHomepagePayload(parsed.value);
    if (directPayload) {
      return directPayload;
    }
  }

  const artifactSnapshot = parseStoredHomepageRenderArtifactSnapshot(parsed.value);
  if (artifactSnapshot) {
    return JSON.stringify(artifactSnapshot);
  }

  return key === SNAPSHOT_KEY ? null : normalizeDirectHomepagePayload(parsed.value);
}

function normalizeHomepageArtifactBodyJson(bodyJson: string): string | null {
  const parsed = parseJsonText(bodyJson);
  if (parsed === null) return null;

  const artifact = publicHomepageStoredRenderArtifactSchema.safeParse(parsed.value);
  if (artifact.success) {
    if (!('snapshot' in artifact.data) && !parseStoredHomepageRenderArtifactSnapshot(artifact.data)) {
      return null;
    }
    return parsed.trimmed;
  }
  if (!isRecord(parsed.value)) {
    return null;
  }

  const version = parsed.value.version;
  if (version !== SPLIT_SNAPSHOT_VERSION && version !== LEGACY_COMBINED_SNAPSHOT_VERSION) {
    return null;
  }

  const legacyArtifact = publicHomepageStoredRenderArtifactSchema.safeParse(parsed.value.render);
  if (!legacyArtifact.success) {
    return null;
  }
  if (
    !('snapshot' in legacyArtifact.data) &&
    !parseStoredHomepageRenderArtifactSnapshot(legacyArtifact.data)
  ) {
    return null;
  }
  return JSON.stringify(legacyArtifact.data);
}

function parseHomepagePayloadSnapshotForKey(
  key: SnapshotKey,
  bodyJson: string,
): PublicHomepageResponse | null {
  const parsed = parseJsonText(bodyJson);
  if (parsed === null) return null;
  if (!isRecord(parsed.value)) {
    return null;
  }

  const version = parsed.value.version;
  if (version === SPLIT_SNAPSHOT_VERSION || version === LEGACY_COMBINED_SNAPSHOT_VERSION) {
    return parseDirectHomepagePayload(parsed.value.data);
  }

  if (key === SNAPSHOT_KEY) {
    const directPayload = parseDirectHomepagePayload(parsed.value);
    if (directPayload) {
      return directPayload;
    }
  }

  const artifactSnapshot = parseStoredHomepageRenderArtifactSnapshot(parsed.value);
  if (artifactSnapshot) {
    return artifactSnapshot;
  }

  return key === SNAPSHOT_KEY ? null : parseDirectHomepagePayload(parsed.value);
}

function toSnapshotUpdatedAt(row: Pick<SnapshotRefreshRow, 'generated_at' | 'updated_at'>): number {
  return typeof row.updated_at === 'number' && Number.isFinite(row.updated_at)
    ? row.updated_at
    : row.generated_at;
}

function getNormalizedSnapshotCache(
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, NormalizedSnapshotRow>>,
  db: D1Database,
): Map<SnapshotKey, NormalizedSnapshotRow> {
  const cached = cacheByDb.get(db);
  if (cached) {
    return cached;
  }

  const next = new Map<SnapshotKey, NormalizedSnapshotRow>();
  cacheByDb.set(db, next);
  return next;
}

function getParsedSnapshotCache(
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, ParsedSnapshotRow>>,
  db: D1Database,
): Map<SnapshotKey, ParsedSnapshotRow> {
  const cached = cacheByDb.get(db);
  if (cached) {
    return cached;
  }

  const next = new Map<SnapshotKey, ParsedSnapshotRow>();
  cacheByDb.set(db, next);
  return next;
}

function readCachedNormalizedSnapshotRow(
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, NormalizedSnapshotRow>>,
  db: D1Database,
  candidate: SnapshotCandidate,
  rawBodyJson: string,
): NormalizedSnapshotRow | null {
  const cache = getNormalizedSnapshotCache(cacheByDb, db);
  const row = cache.get(candidate.key);
  if (!row) {
    return null;
  }

  return row.generatedAt === candidate.generatedAt &&
    row.updatedAt === candidate.updatedAt &&
    row.rawBodyJson === rawBodyJson
    ? row
    : null;
}

function writeCachedNormalizedSnapshotRow(
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, NormalizedSnapshotRow>>,
  db: D1Database,
  candidate: SnapshotCandidate,
  rawBodyJson: string,
  bodyJson: string,
): NormalizedSnapshotRow {
  const row: NormalizedSnapshotRow = {
    generatedAt: candidate.generatedAt,
    updatedAt: candidate.updatedAt,
    rawBodyJson,
    bodyJson,
  };
  getNormalizedSnapshotCache(cacheByDb, db).set(candidate.key, row);
  return row;
}

function readCachedParsedSnapshotRow(
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, ParsedSnapshotRow>>,
  db: D1Database,
  candidate: SnapshotCandidate,
  rawBodyJson: string,
): ParsedSnapshotRow | null {
  const cache = getParsedSnapshotCache(cacheByDb, db);
  const row = cache.get(candidate.key);
  if (!row) {
    return null;
  }

  return row.generatedAt === candidate.generatedAt &&
    row.updatedAt === candidate.updatedAt &&
    row.rawBodyJson === rawBodyJson
    ? row
    : null;
}

function writeCachedParsedSnapshotRow(
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, ParsedSnapshotRow>>,
  db: D1Database,
  candidate: SnapshotCandidate,
  rawBodyJson: string,
  snapshot: PublicHomepageResponse,
): ParsedSnapshotRow {
  const row: ParsedSnapshotRow = {
    generatedAt: candidate.generatedAt,
    updatedAt: candidate.updatedAt,
    rawBodyJson,
    snapshot,
  };
  getParsedSnapshotCache(cacheByDb, db).set(candidate.key, row);
  return row;
}

function readCachedNormalizedSnapshotRowGlobal(
  cache: ReadonlyMap<SnapshotKey, RawNormalizedSnapshotRow>,
  candidate: SnapshotCandidate,
  rawBodyJson: string,
): NormalizedSnapshotRow | null {
  const row = cache.get(candidate.key);
  if (!row) {
    return null;
  }

  return row.generatedAt === candidate.generatedAt &&
    row.updatedAt === candidate.updatedAt &&
    row.rawBodyJson === rawBodyJson
    ? row
    : null;
}

function writeCachedNormalizedSnapshotRowGlobal(
  cache: Map<SnapshotKey, RawNormalizedSnapshotRow>,
  candidate: SnapshotCandidate,
  rawBodyJson: string,
  bodyJson: string,
): RawNormalizedSnapshotRow {
  const row: RawNormalizedSnapshotRow = {
    generatedAt: candidate.generatedAt,
    updatedAt: candidate.updatedAt,
    rawBodyJson,
    bodyJson,
  };
  cache.set(candidate.key, row);
  return row;
}

function readCachedParsedSnapshotRowGlobal(
  cache: ReadonlyMap<SnapshotKey, RawParsedSnapshotRow>,
  candidate: SnapshotCandidate,
  rawBodyJson: string,
): ParsedSnapshotRow | null {
  const row = cache.get(candidate.key);
  if (!row) {
    return null;
  }

  return row.generatedAt === candidate.generatedAt &&
    row.updatedAt === candidate.updatedAt &&
    row.rawBodyJson === rawBodyJson
    ? row
    : null;
}

function writeCachedParsedSnapshotRowGlobal(
  cache: Map<SnapshotKey, RawParsedSnapshotRow>,
  candidate: SnapshotCandidate,
  rawBodyJson: string,
  snapshot: PublicHomepageResponse,
): RawParsedSnapshotRow {
  const row: RawParsedSnapshotRow = {
    generatedAt: candidate.generatedAt,
    updatedAt: candidate.updatedAt,
    rawBodyJson,
    snapshot,
  };
  cache.set(candidate.key, row);
  return row;
}

function isSameUtcDay(a: number, b: number): boolean {
  return Math.floor(a / 86_400) === Math.floor(b / 86_400);
}

function isFutureSnapshotCandidate(candidate: SnapshotCandidate, now: number): boolean {
  return candidate.generatedAt > now + MAX_FUTURE_SNAPSHOT_SKEW_SECONDS;
}

function snapshotCandidateAgeSeconds(candidate: SnapshotCandidate, now: number): number {
  return Math.max(0, now - candidate.generatedAt);
}

async function readRefreshSnapshotRows(
  db: D1Database,
): Promise<SnapshotRefreshRow[]> {
  try {
    const cached = readRefreshSnapshotRowsStatementByDb.get(db);
    const statement = cached ?? db.prepare(READ_REFRESH_SNAPSHOT_ROWS_SQL);
    if (!cached) {
      readRefreshSnapshotRowsStatementByDb.set(db, statement);
    }

    const { results } = await statement
      .bind(SNAPSHOT_KEY, SNAPSHOT_ARTIFACT_KEY)
      .all<SnapshotRefreshRow>();
    return results ?? [];
  } catch (err) {
    console.warn('homepage snapshot: refresh rows read failed', err);
    return [];
  }
}

async function readRefreshSnapshotMetadataRows(
  db: D1Database,
): Promise<SnapshotRefreshMetadataRow[]> {
  try {
    const cached = readRefreshSnapshotMetadataStatementByDb.get(db);
    const statement = cached ?? db.prepare(READ_REFRESH_SNAPSHOT_METADATA_SQL);
    if (!cached) {
      readRefreshSnapshotMetadataStatementByDb.set(db, statement);
    }

    const { results } = await statement
      .bind(SNAPSHOT_KEY, SNAPSHOT_ARTIFACT_KEY)
      .all<SnapshotRefreshMetadataRow>();
    return results ?? [];
  } catch (err) {
    console.warn('homepage snapshot: refresh metadata read failed', err);
    return [];
  }
}

async function readRefreshSnapshotRowByKey(
  db: D1Database,
  key: SnapshotKey,
): Promise<SnapshotRefreshRow | null> {
  try {
    const cached = readRefreshSnapshotRowByKeyStatementByDb.get(db);
    const statement = cached ?? db.prepare(READ_REFRESH_SNAPSHOT_ROW_BY_KEY_SQL);
    if (!cached) {
      readRefreshSnapshotRowByKeyStatementByDb.set(db, statement);
    }

    const row = await statement.bind(key).first<Omit<SnapshotRefreshRow, 'key'>>();
    return row ? { key, ...row } : null;
  } catch (err) {
    console.warn('homepage snapshot: refresh row read failed', err);
    return null;
  }
}

function listSnapshotCandidatesFromRefreshRows(
  rows: readonly SnapshotRefreshMetadataRow[],
): SnapshotCandidate[] {
  return rows.map((row) => ({
    key: row.key,
    generatedAt: row.generated_at,
    updatedAt: toSnapshotUpdatedAt(row),
  }));
}

function comparePayloadCandidates(a: SnapshotCandidate, b: SnapshotCandidate): number {
  if (a.generatedAt !== b.generatedAt) {
    return b.generatedAt - a.generatedAt;
  }
  if (a.key === b.key) {
    return 0;
  }
  return a.key === SNAPSHOT_KEY ? -1 : 1;
}

function compareArtifactCandidates(a: SnapshotCandidate, b: SnapshotCandidate): number {
  if (a.generatedAt !== b.generatedAt) {
    return b.generatedAt - a.generatedAt;
  }
  if (a.key === b.key) {
    return 0;
  }
  return a.key === SNAPSHOT_ARTIFACT_KEY ? -1 : 1;
}

function readValidatedSnapshotCandidateFromRefreshRows(opts: {
  db: D1Database;
  candidate: SnapshotCandidate;
  rowByKey: ReadonlyMap<SnapshotKey, SnapshotRefreshRow>;
  normalize: (candidate: SnapshotCandidate, bodyJson: string) => string | null;
  cacheByDb: WeakMap<D1Database, Map<SnapshotKey, NormalizedSnapshotRow>>;
  globalCache: Map<SnapshotKey, RawNormalizedSnapshotRow>;
}): CandidateReadResult {
  const row = opts.rowByKey.get(opts.candidate.key);
  if (!row || row.generated_at !== opts.candidate.generatedAt) {
    return { row: null, invalid: false };
  }

  const dbCached = readCachedNormalizedSnapshotRow(
    opts.cacheByDb,
    opts.db,
    opts.candidate,
    row.body_json,
  );
  if (dbCached) {
    return { row: dbCached, invalid: false };
  }

  const globalCached = readCachedNormalizedSnapshotRowGlobal(
    opts.globalCache,
    opts.candidate,
    row.body_json,
  );
  if (globalCached) {
    return {
      row: writeCachedNormalizedSnapshotRow(
        opts.cacheByDb,
        opts.db,
        opts.candidate,
        row.body_json,
        globalCached.bodyJson,
      ),
      invalid: false,
    };
  }

  const bodyJson = opts.normalize(opts.candidate, row.body_json);
  if (!bodyJson) {
    return { row: null, invalid: true };
  }

  writeCachedNormalizedSnapshotRowGlobal(
    opts.globalCache,
    opts.candidate,
    row.body_json,
    bodyJson,
  );
  return {
    row: writeCachedNormalizedSnapshotRow(
      opts.cacheByDb,
      opts.db,
      opts.candidate,
      row.body_json,
      bodyJson,
    ),
    invalid: false,
  };
}

export async function readHomepageSnapshotGeneratedAt(
  db: D1Database,
  now = Math.floor(Date.now() / 1000),
): Promise<number | null> {
  const candidates = listSnapshotCandidatesFromRefreshRows(await readRefreshSnapshotMetadataRows(db))
    .filter((candidate) => !isFutureSnapshotCandidate(candidate, now))
    .sort(comparePayloadCandidates);

  for (const candidate of candidates) {
    const row = await readRefreshSnapshotRowByKey(db, candidate.key);
    if (!row) {
      continue;
    }

    const liveCandidate: SnapshotCandidate = {
      key: candidate.key,
      generatedAt: row.generated_at,
      updatedAt: toSnapshotUpdatedAt(row),
    };
    if (isFutureSnapshotCandidate(liveCandidate, now)) {
      continue;
    }

    const dbCached = readCachedNormalizedSnapshotRow(
      normalizedHomepagePayloadCacheByDb,
      db,
      liveCandidate,
      row.body_json,
    );
    if (dbCached) {
      return dbCached.generatedAt;
    }

    const globalCached = readCachedNormalizedSnapshotRowGlobal(
      normalizedHomepagePayloadCacheGlobal,
      liveCandidate,
      row.body_json,
    );
    if (globalCached) {
      writeCachedNormalizedSnapshotRow(
        normalizedHomepagePayloadCacheByDb,
        db,
        liveCandidate,
        row.body_json,
        globalCached.bodyJson,
      );
      return liveCandidate.generatedAt;
    }

    const bodyJson = normalizeHomepagePayloadBodyJsonForKey(liveCandidate.key, row.body_json);
    if (!bodyJson) {
      continue;
    }

    writeCachedNormalizedSnapshotRowGlobal(
      normalizedHomepagePayloadCacheGlobal,
      liveCandidate,
      row.body_json,
      bodyJson,
    );
    writeCachedNormalizedSnapshotRow(
      normalizedHomepagePayloadCacheByDb,
      db,
      liveCandidate,
      row.body_json,
      bodyJson,
    );
    return liveCandidate.generatedAt;
  }

  return null;
}

export async function readHomepageArtifactSnapshotGeneratedAt(
  db: D1Database,
): Promise<number | null> {
  const refreshRows = await readRefreshSnapshotRows(db);
  const rowByKey = new Map(refreshRows.map((row) => [row.key, row]));
  const candidates = listSnapshotCandidatesFromRefreshRows(refreshRows)
    .filter((candidate) => candidate.key === SNAPSHOT_ARTIFACT_KEY)
    .sort(compareArtifactCandidates);

  for (const candidate of candidates) {
    const result = readValidatedSnapshotCandidateFromRefreshRows({
      db,
      candidate,
      rowByKey,
      normalize: (_candidate, bodyJson) => normalizeHomepageArtifactBodyJson(bodyJson),
      cacheByDb: normalizedHomepageArtifactCacheByDb,
      globalCache: normalizedHomepageArtifactCacheGlobal,
    });
    if (result.row) {
      return result.row.generatedAt;
    }
  }

  return null;
}

export async function readHomepageRefreshBaseSnapshot(
  db: D1Database,
  now: number,
): Promise<{
  generatedAt: number | null;
  snapshot: PublicHomepageResponse | null;
  seedDataSnapshot: boolean;
}> {
  let invalid = false;
  const parsedByKey = new Map<SnapshotKey, ParsedSnapshotRow | null>();
  const rowByKey = new Map<SnapshotKey, SnapshotRefreshRow | null>();

  const readRefreshCandidate = async (
    candidate: SnapshotCandidate,
  ): Promise<ParsedSnapshotRow | null> => {
    if (parsedByKey.has(candidate.key)) {
      return parsedByKey.get(candidate.key) ?? null;
    }

    if (isFutureSnapshotCandidate(candidate, now)) {
      invalid = true;
      parsedByKey.set(candidate.key, null);
      return null;
    }

    let row = rowByKey.get(candidate.key);
    if (row === undefined) {
      row = await readRefreshSnapshotRowByKey(db, candidate.key);
      rowByKey.set(candidate.key, row ?? null);
    }

    if (!row?.body_json || row.generated_at !== candidate.generatedAt) {
      parsedByKey.set(candidate.key, null);
      return null;
    }

    const dbCached = readCachedParsedSnapshotRow(
      parsedHomepagePayloadCacheByDb,
      db,
      candidate,
      row.body_json,
    );
    if (dbCached) {
      parsedByKey.set(candidate.key, dbCached);
      return dbCached;
    }

    const globalCached = readCachedParsedSnapshotRowGlobal(
      parsedHomepagePayloadCacheGlobal,
      candidate,
      row.body_json,
    );
    if (globalCached) {
      const parsedRow = writeCachedParsedSnapshotRow(
        parsedHomepagePayloadCacheByDb,
        db,
        candidate,
        row.body_json,
        globalCached.snapshot,
      );
      parsedByKey.set(candidate.key, parsedRow);
      return parsedRow;
    }

    const snapshot = parseHomepagePayloadSnapshotForKey(candidate.key, row.body_json);
    if (!snapshot) {
      invalid = true;
      parsedByKey.set(candidate.key, null);
      return null;
    }

    writeCachedParsedSnapshotRowGlobal(
      parsedHomepagePayloadCacheGlobal,
      candidate,
      row.body_json,
      snapshot,
    );
    const parsedRow = writeCachedParsedSnapshotRow(
      parsedHomepagePayloadCacheByDb,
      db,
      candidate,
      row.body_json,
      snapshot,
    );
    parsedByKey.set(candidate.key, parsedRow);
    return parsedRow;
  };

  const refreshMetadataRows = await readRefreshSnapshotMetadataRows(db);
  const candidateByKey = new Map(
    listSnapshotCandidatesFromRefreshRows(refreshMetadataRows).map((candidate) => [candidate.key, candidate]),
  );
  const homepageCandidate = candidateByKey.get(SNAPSHOT_KEY) ?? null;
  const artifactCandidate = candidateByKey.get(SNAPSHOT_ARTIFACT_KEY) ?? null;

  if (homepageCandidate && isSameUtcDay(homepageCandidate.generatedAt, now)) {
    const homepageBase = await readRefreshCandidate(homepageCandidate);
    if (homepageBase) {
      if (
        !artifactCandidate ||
        !isSameUtcDay(artifactCandidate.generatedAt, now) ||
        comparePayloadCandidates(artifactCandidate, homepageCandidate) >= 0
      ) {
        return {
          generatedAt: homepageBase.generatedAt,
          snapshot: homepageBase.snapshot,
          seedDataSnapshot: false,
        };
      }
    }
  }

  if (artifactCandidate && isSameUtcDay(artifactCandidate.generatedAt, now)) {
    const artifactBase = await readRefreshCandidate(artifactCandidate);
    if (artifactBase) {
      return {
        generatedAt: artifactBase.generatedAt,
        snapshot: artifactBase.snapshot,
        seedDataSnapshot: false,
      };
    }
  }

  const orderedCandidates = [homepageCandidate, artifactCandidate]
    .filter((candidate): candidate is SnapshotCandidate => candidate !== null)
    .sort(comparePayloadCandidates);

  for (const candidate of orderedCandidates) {
    const freshestBase = await readRefreshCandidate(candidate);
    if (!freshestBase) {
      continue;
    }

    return {
      generatedAt: freshestBase.generatedAt,
      snapshot: freshestBase.snapshot,
      seedDataSnapshot: true,
    };
  }

  if (invalid) {
    console.warn('homepage snapshot: invalid refresh payload');
  }

  return {
    generatedAt: null,
    snapshot: null,
    seedDataSnapshot: true,
  };
}

export function primeHomepageRefreshBaseSnapshotCache(opts: {
  db: D1Database;
  generatedAt: number;
  updatedAt: number;
  snapshot: PublicHomepageResponse;
  renderBodyJson: string;
  payloadBodyJson?: string | null;
}): void {
  const artifactCandidate: SnapshotCandidate = {
    key: SNAPSHOT_ARTIFACT_KEY,
    generatedAt: opts.generatedAt,
    updatedAt: opts.updatedAt,
  };
  writeCachedParsedSnapshotRowGlobal(
    parsedHomepagePayloadCacheGlobal,
    artifactCandidate,
    opts.renderBodyJson,
    opts.snapshot,
  );
  writeCachedParsedSnapshotRow(
    parsedHomepagePayloadCacheByDb,
    opts.db,
    artifactCandidate,
    opts.renderBodyJson,
    opts.snapshot,
  );

  if (!opts.payloadBodyJson) {
    return;
  }

  const payloadCandidate: SnapshotCandidate = {
    key: SNAPSHOT_KEY,
    generatedAt: opts.generatedAt,
    updatedAt: opts.updatedAt,
  };
  writeCachedParsedSnapshotRowGlobal(
    parsedHomepagePayloadCacheGlobal,
    payloadCandidate,
    opts.payloadBodyJson,
    opts.snapshot,
  );
  writeCachedParsedSnapshotRow(
    parsedHomepagePayloadCacheByDb,
    opts.db,
    payloadCandidate,
    opts.payloadBodyJson,
    opts.snapshot,
  );
}

export function applyHomepageCacheHeaders(res: Response, ageSeconds: number): void {
  const remaining = Math.max(0, MAX_AGE_SECONDS - ageSeconds);
  const maxAge = Math.min(30, remaining);
  const stale = Math.max(0, remaining - maxAge);

  res.headers.set(
    'Cache-Control',
    `public, max-age=${maxAge}, stale-while-revalidate=${stale}, stale-if-error=${stale}`,
  );
}

export async function readHomepageSnapshotJsonAnyAge(
  db: D1Database,
  now: number,
  maxStaleSeconds = MAX_STALE_SECONDS,
): Promise<{ bodyJson: string; age: number } | null> {
  const refreshRows = await readRefreshSnapshotRows(db);
  const rowByKey = new Map(refreshRows.map((row) => [row.key, row]));
  const candidates = listSnapshotCandidatesFromRefreshRows(refreshRows)
    .filter(
      (candidate) =>
        !isFutureSnapshotCandidate(candidate, now) &&
        snapshotCandidateAgeSeconds(candidate, now) <= maxStaleSeconds,
    )
    .sort(comparePayloadCandidates);

  for (const candidate of candidates) {
    const result = readValidatedSnapshotCandidateFromRefreshRows({
      db,
      candidate,
      rowByKey,
      normalize: (currentCandidate, bodyJson) =>
        normalizeHomepagePayloadBodyJsonForKey(currentCandidate.key, bodyJson),
      cacheByDb: normalizedHomepagePayloadCacheByDb,
      globalCache: normalizedHomepagePayloadCacheGlobal,
    });
    if (!result.row) {
      if (result.invalid) {
        console.warn('homepage snapshot: invalid payload');
      }
      continue;
    }

    return {
      bodyJson: result.row.bodyJson,
      age: snapshotCandidateAgeSeconds(
        {
          key: candidate.key,
          generatedAt: result.row.generatedAt,
          updatedAt: result.row.updatedAt,
        },
        now,
      ),
    };
  }

  return null;
}

export async function readHomepageSnapshotJson(
  db: D1Database,
  now: number,
): Promise<{ bodyJson: string; age: number } | null> {
  return await readHomepageSnapshotJsonAnyAge(db, now, MAX_AGE_SECONDS);
}

export async function readHomepageSnapshotArtifactJson(
  db: D1Database,
  now: number,
): Promise<{ bodyJson: string; age: number } | null> {
  const refreshRows = await readRefreshSnapshotRows(db);
  const rowByKey = new Map(refreshRows.map((row) => [row.key, row]));
  const candidates = listSnapshotCandidatesFromRefreshRows(refreshRows)
    .filter(
      (candidate) =>
        !isFutureSnapshotCandidate(candidate, now) &&
        snapshotCandidateAgeSeconds(candidate, now) <= MAX_AGE_SECONDS,
    )
    .sort(compareArtifactCandidates);

  for (const candidate of candidates) {
    const result = readValidatedSnapshotCandidateFromRefreshRows({
      db,
      candidate,
      rowByKey,
      normalize: (_candidate, bodyJson) => normalizeHomepageArtifactBodyJson(bodyJson),
      cacheByDb: normalizedHomepageArtifactCacheByDb,
      globalCache: normalizedHomepageArtifactCacheGlobal,
    });
    if (!result.row) {
      if (result.invalid) {
        console.warn('homepage snapshot: invalid artifact payload');
      }
      continue;
    }

    return {
      bodyJson: result.row.bodyJson,
      age: snapshotCandidateAgeSeconds(
        {
          key: candidate.key,
          generatedAt: result.row.generatedAt,
          updatedAt: result.row.updatedAt,
        },
        now,
      ),
    };
  }

  return null;
}

export async function readStaleHomepageSnapshotArtifactJson(
  db: D1Database,
  now: number,
): Promise<{ bodyJson: string; age: number } | null> {
  const refreshRows = await readRefreshSnapshotRows(db);
  const rowByKey = new Map(refreshRows.map((row) => [row.key, row]));
  const candidates = listSnapshotCandidatesFromRefreshRows(refreshRows)
    .filter((candidate) => Math.max(0, now - candidate.generatedAt) <= MAX_STALE_SECONDS)
    .sort(compareArtifactCandidates);

  for (const candidate of candidates) {
    const result = readValidatedSnapshotCandidateFromRefreshRows({
      db,
      candidate,
      rowByKey,
      normalize: (_candidate, bodyJson) => normalizeHomepageArtifactBodyJson(bodyJson),
      cacheByDb: normalizedHomepageArtifactCacheByDb,
      globalCache: normalizedHomepageArtifactCacheGlobal,
    });
    if (!result.row) {
      if (result.invalid) {
        console.warn('homepage snapshot: invalid stale artifact payload');
      }
      continue;
    }

    return {
      bodyJson: result.row.bodyJson,
      age: Math.max(0, now - result.row.generatedAt),
    };
  }

  return null;
}

export function assertHomepageArtifactAvailable(): never {
  throw new AppError(503, 'UNAVAILABLE', 'Homepage artifact unavailable');
}
