import { randomUUID } from "node:crypto";

import { getBrowsePageFromEntries, toRepoKey } from "~/features/browse/catalog";
import type {
  BrowseIndexEntry,
  BrowsePageResult,
  BrowseQuery,
  RecentBrowseIndex,
} from "~/features/browse/catalog";
import { readRequiredEnv } from "./config";
import { withDistributedLock } from "./distributed-lock";
import {
  deleteObject,
  getGzipJsonObject,
  getJsonObject,
  putGzipJsonObject,
} from "./r2";

const LEGACY_PUBLIC_BROWSE_INDEX_KEY = "public/v1/_meta/browse-index.json";
const PUBLIC_BROWSE_INDEX_KEY = "public/v2/_meta/browse-index.json.gz";
const PUBLIC_RECENT_BROWSE_INDEX_KEY = "public/v2/_meta/browse-recent.json.gz";
const PUBLIC_BROWSE_MANIFEST_KEY =
  "public/v3/_meta/browse-index-manifest.json.gz";
const PUBLIC_BROWSE_SNAPSHOT_PREFIX = "public/v3/_meta/browse-index-snapshot-";
const PUBLIC_BROWSE_INDEX_LOCK_KEY = "lock:v1:public-browse-index";
const BROWSE_INDEX_WRITE_ATTEMPTS = 3;
const BROWSE_INDEX_LOCK_TTL_MS = 60_000;
const BROWSE_INDEX_LOCK_WAIT_MS = 10_000;
const BROWSE_INDEX_RETAINED_SNAPSHOTS = 8;
export const RECENT_BROWSE_INDEX_SIZE = 2_000;

export type { BrowseIndexEntry, BrowsePageResult, BrowseQuery };

interface LegacyBrowseIndexPayload {
  version: 1 | 2;
  updatedAt: string;
  entries: BrowseIndexEntry[];
}

interface RecentBrowseIndexPayload extends RecentBrowseIndex {
  version: 1;
  updatedAt: string;
}

interface BrowseIndexSnapshotPayload {
  version: 3;
  generation: string;
  updatedAt: string;
  entries: BrowseIndexEntry[];
}

interface BrowseIndexManifestPayload extends RecentBrowseIndex {
  version: 3;
  generation: string;
  updatedAt: string;
  snapshotKey: string;
  retainedSnapshotKeys?: string[];
}

interface StoredBrowseIndex {
  entries: BrowseIndexEntry[];
  activeSnapshotKey: string | null;
  retainedSnapshotKeys: string[];
}

type PutGzipJsonObjectFn = typeof putGzipJsonObject;
type DeleteObjectFn = typeof deleteObject;
type ReadJsonObjectFn = <T>(bucket: string, key: string) => Promise<T | null>;

export class BrowseIndexNotFoundError extends Error {
  constructor() {
    super(
      `Browse index missing at ${PUBLIC_BROWSE_MANIFEST_KEY} and legacy fallbacks.`,
    );
    this.name = "BrowseIndexNotFoundError";
  }
}

function getPublicBucket(): string {
  return readRequiredEnv("R2_PUBLIC_BUCKET");
}

function compareIsoDatesDescending(left: string, right: string) {
  const difference = Date.parse(right) - Date.parse(left);
  return Number.isFinite(difference) && difference !== 0 ? difference : 0;
}

function compareBrowseEntriesByRecent(
  left: BrowseIndexEntry,
  right: BrowseIndexEntry,
) {
  return (
    compareIsoDatesDescending(left.lastSuccessfulAt, right.lastSuccessfulAt) ||
    toRepoKey(left).localeCompare(toRepoKey(right))
  );
}

function normalizeBrowseIndexEntry(entry: BrowseIndexEntry): BrowseIndexEntry {
  return {
    username: entry.username.trim().toLowerCase(),
    repo: entry.repo.trim().toLowerCase(),
    lastSuccessfulAt: entry.lastSuccessfulAt,
    stargazerCount:
      typeof entry.stargazerCount === "number" ? entry.stargazerCount : null,
  };
}

function pickPreferredEntry(
  existing: BrowseIndexEntry | undefined,
  incoming: BrowseIndexEntry,
): BrowseIndexEntry {
  if (!existing) {
    return incoming;
  }

  const existingTime = Date.parse(existing.lastSuccessfulAt);
  const incomingTime = Date.parse(incoming.lastSuccessfulAt);

  if (Number.isFinite(incomingTime) && incomingTime > existingTime) {
    return incoming;
  }

  if (
    incomingTime === existingTime &&
    existing.stargazerCount === null &&
    incoming.stargazerCount !== null
  ) {
    return incoming;
  }

  return existing;
}

function normalizeBrowseIndexEntries(
  entries: BrowseIndexEntry[],
): BrowseIndexEntry[] {
  const deduped = new Map<string, BrowseIndexEntry>();

  for (const rawEntry of entries) {
    const entry = normalizeBrowseIndexEntry(rawEntry);
    const repoKey = toRepoKey(entry);
    deduped.set(repoKey, pickPreferredEntry(deduped.get(repoKey), entry));
  }

  return Array.from(deduped.values()).sort(compareBrowseEntriesByRecent);
}

function insertRecentEntry(
  entries: BrowseIndexEntry[],
  entry: BrowseIndexEntry,
) {
  let low = 0;
  let high = entries.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    const candidate = entries[middle];
    if (candidate && compareBrowseEntriesByRecent(candidate, entry) <= 0) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  entries.splice(low, 0, entry);
}

function createBrowseSnapshotKey(generation: string): string {
  return `${PUBLIC_BROWSE_SNAPSHOT_PREFIX}${generation}.json.gz`;
}

function isBrowseSnapshotKey(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.startsWith(PUBLIC_BROWSE_SNAPSHOT_PREFIX) &&
    value.endsWith(".json.gz")
  );
}

async function readStoredBrowseIndex(): Promise<StoredBrowseIndex | null> {
  return readStoredBrowseIndexWith(getGzipJsonObject, getJsonObject);
}

export async function readBrowseIndex(): Promise<BrowseIndexEntry[] | null> {
  return (await readStoredBrowseIndex())?.entries ?? null;
}

export async function readRecentBrowseIndex(): Promise<RecentBrowseIndex | null> {
  const manifest = await getGzipJsonObject<BrowseIndexManifestPayload>(
    getPublicBucket(),
    PUBLIC_BROWSE_MANIFEST_KEY,
  );
  if (
    manifest?.version === 3 &&
    typeof manifest.generation === "string" &&
    isBrowseSnapshotKey(manifest.snapshotKey)
  ) {
    return {
      entries: manifest.entries ?? [],
      total: manifest.total ?? 0,
    };
  }

  const stored = await getGzipJsonObject<RecentBrowseIndexPayload>(
    getPublicBucket(),
    PUBLIC_RECENT_BROWSE_INDEX_KEY,
  );
  if (!stored) {
    return null;
  }

  return {
    entries: stored.entries ?? [],
    total: stored.total ?? 0,
  };
}

export async function migrateBrowseIndexToAtomicV3(): Promise<number> {
  return withDistributedLock({
    key: PUBLIC_BROWSE_INDEX_LOCK_KEY,
    ttlMs: BROWSE_INDEX_LOCK_TTL_MS,
    waitMs: BROWSE_INDEX_LOCK_WAIT_MS,
    callback: async () => {
      const stored = await readStoredBrowseIndex();
      if (!stored) {
        throw new BrowseIndexNotFoundError();
      }
      if (stored.activeSnapshotKey) {
        return stored.entries.length;
      }

      const entries = normalizeBrowseIndexEntries(stored.entries);
      return (
        await writeBrowseIndex({
          entries,
          retainedSnapshotKeys: [],
        })
      ).length;
    },
  });
}

async function readStoredBrowseIndexWith(
  getGzipJsonObjectFn: ReadJsonObjectFn,
  getJsonObjectFn: ReadJsonObjectFn,
): Promise<StoredBrowseIndex | null> {
  const manifest = await getGzipJsonObjectFn<BrowseIndexManifestPayload>(
    getPublicBucket(),
    PUBLIC_BROWSE_MANIFEST_KEY,
  );
  if (
    manifest?.version === 3 &&
    typeof manifest.generation === "string" &&
    isBrowseSnapshotKey(manifest.snapshotKey)
  ) {
    const snapshot = await getGzipJsonObjectFn<BrowseIndexSnapshotPayload>(
      getPublicBucket(),
      manifest.snapshotKey,
    );
    if (
      snapshot?.version === 3 &&
      snapshot.generation === manifest.generation
    ) {
      return {
        entries: snapshot.entries ?? [],
        activeSnapshotKey: manifest.snapshotKey,
        retainedSnapshotKeys: [
          manifest.snapshotKey,
          ...(manifest.retainedSnapshotKeys ?? []).filter(isBrowseSnapshotKey),
        ].filter((key, index, keys) => keys.indexOf(key) === index),
      };
    }

    console.error(
      JSON.stringify({
        event: "browse.index.snapshot_invalid",
        generation: manifest.generation,
      }),
    );
  }

  const compressed = await getGzipJsonObjectFn<LegacyBrowseIndexPayload>(
    getPublicBucket(),
    PUBLIC_BROWSE_INDEX_KEY,
  );
  const stored =
    compressed ??
    (await getJsonObjectFn<LegacyBrowseIndexPayload>(
      getPublicBucket(),
      LEGACY_PUBLIC_BROWSE_INDEX_KEY,
    ));

  if (!stored) {
    return null;
  }

  return {
    entries:
      stored.version === 2
        ? (stored.entries ?? [])
        : normalizeBrowseIndexEntries(stored.entries ?? []),
    activeSnapshotKey: null,
    retainedSnapshotKeys: [],
  };
}

async function writeBrowseIndex(
  params: {
    entries: BrowseIndexEntry[];
    retainedSnapshotKeys: string[];
  },
  dependencies: {
    putGzipJsonObjectFn?: PutGzipJsonObjectFn;
    deleteObjectFn?: DeleteObjectFn;
    now?: Date;
    generation?: string;
  } = {},
): Promise<BrowseIndexEntry[]> {
  const putGzipJsonObjectFn =
    dependencies.putGzipJsonObjectFn ?? putGzipJsonObject;
  const deleteObjectFn = dependencies.deleteObjectFn ?? deleteObject;
  const now = dependencies.now ?? new Date();
  const generation = dependencies.generation ?? randomUUID();
  const updatedAt = now.toISOString();
  const snapshotKey = createBrowseSnapshotKey(generation);
  const retainedSnapshotKeys = [snapshotKey, ...params.retainedSnapshotKeys]
    .filter((key, index, keys) => keys.indexOf(key) === index)
    .slice(0, BROWSE_INDEX_RETAINED_SNAPSHOTS);
  const retiredSnapshotKeys = params.retainedSnapshotKeys.filter(
    (key) => !retainedSnapshotKeys.includes(key),
  );

  // The manifest is the atomic commit point. It is published only after the
  // full snapshot exists and embeds the recent projection, so readers cannot
  // observe mismatched full and recent indexes.
  await putGzipJsonObjectFn(getPublicBucket(), snapshotKey, {
    version: 3,
    generation,
    updatedAt,
    entries: params.entries,
  } satisfies BrowseIndexSnapshotPayload);
  await putGzipJsonObjectFn(getPublicBucket(), PUBLIC_BROWSE_MANIFEST_KEY, {
    version: 3,
    generation,
    updatedAt,
    snapshotKey,
    retainedSnapshotKeys,
    total: params.entries.length,
    entries: params.entries.slice(0, RECENT_BROWSE_INDEX_SIZE),
  } satisfies BrowseIndexManifestPayload);

  for (const retiredSnapshotKey of retiredSnapshotKeys) {
    try {
      await deleteObjectFn(getPublicBucket(), retiredSnapshotKey);
    } catch (error) {
      console.warn(
        JSON.stringify({
          event: "browse.index.snapshot_cleanup_failed",
          snapshot_key: retiredSnapshotKey,
          error: error instanceof Error ? error.message : "Unknown error",
        }),
      );
    }
  }

  return params.entries;
}

export async function upsertBrowseIndexEntry(
  entry: BrowseIndexEntry,
): Promise<BrowseIndexEntry[]> {
  const generation = randomUUID();
  let lastError: unknown;

  for (let attempt = 1; attempt <= BROWSE_INDEX_WRITE_ATTEMPTS; attempt++) {
    try {
      return await withDistributedLock({
        key: PUBLIC_BROWSE_INDEX_LOCK_KEY,
        ttlMs: BROWSE_INDEX_LOCK_TTL_MS,
        waitMs: BROWSE_INDEX_LOCK_WAIT_MS,
        callback: async () => {
          const stored = await readStoredBrowseIndex();
          const existingEntries = stored?.entries ?? [];
          const normalizedEntry = normalizeBrowseIndexEntry(entry);
          const existingIndex = existingEntries.findIndex(
            (candidate) =>
              candidate.username === normalizedEntry.username &&
              candidate.repo === normalizedEntry.repo,
          );
          const existingEntry = existingEntries[existingIndex];

          if (
            existingEntry &&
            pickPreferredEntry(existingEntry, normalizedEntry) === existingEntry
          ) {
            if (stored?.activeSnapshotKey) {
              return existingEntries;
            }
            return writeBrowseIndex(
              {
                entries: existingEntries,
                retainedSnapshotKeys: [],
              },
              { generation },
            );
          }

          if (existingIndex >= 0) {
            existingEntries.splice(existingIndex, 1);
          }
          insertRecentEntry(existingEntries, normalizedEntry);
          return writeBrowseIndex(
            {
              entries: existingEntries,
              retainedSnapshotKeys: stored?.retainedSnapshotKeys ?? [],
            },
            { generation },
          );
        },
      });
    } catch (error) {
      lastError = error;
      if (attempt < BROWSE_INDEX_WRITE_ATTEMPTS) {
        await new Promise((resolve) => setTimeout(resolve, attempt * 100));
      }
    }
  }

  throw lastError;
}

export async function getBrowsePage(
  query: BrowseQuery,
): Promise<BrowsePageResult> {
  const stored = await readStoredBrowseIndex();
  if (!stored) {
    throw new BrowseIndexNotFoundError();
  }

  return getBrowsePageFromEntries(stored.entries, query);
}
