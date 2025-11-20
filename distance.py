#!/usr/bin/env python3
"""
Robust async OSRM table batcher with logging and fallback per-pair requests.

Features:
- conservative defaults for public router.project-osrm.org
- logs failing chunk requests to logs/
- retries with exponential backoff
- fallback single-pair synchronous calls for any still-missing pairs
- persistent shelve cache
- preserves invalid input rows (keeps them blank in output)

Dependencies:
pip install aiohttp pandas tqdm openpyxl requests
"""

import asyncio
import aiohttp
import requests
import pandas as pd
import math
import shelve
import time
import os
import json
from tqdm import tqdm

OSRM_BASE = "https://router.project-osrm.org"
TABLE_PATH = "/table/v1/driving/"
CACHE_FILE = "route_cache.db"
LOG_DIR = "logs"

# Conservative defaults (public OSRM)
MAX_NODES_PER_REQUEST = 20
MAX_CONCURRENT_REQUESTS = 2
PER_REQUEST_TIMEOUT = 30
RETRY = 3
BACKOFF = 0.6
FALLBACK_SLEEP = 0.2  # between synchronous fallback calls

os.makedirs(LOG_DIR, exist_ok=True)

# -------------------------
# Cache helpers
# -------------------------
def load_cache_to_dict(cache_file):
    d = {}
    if os.path.exists(cache_file):
        try:
            with shelve.open(cache_file) as s:
                for k in s:
                    d[k] = s[k]
        except Exception:
            d = {}
    return d

def dump_cache_from_dict(cache_file, cache_dict):
    with shelve.open(cache_file) as s:
        for k, v in cache_dict.items():
            s[k] = v

def clear_cache_file(cache_file):
    for fn in [cache_file, cache_file + ".db", cache_file + ".bak", cache_file + ".dat", cache_file + ".dir"]:
        if os.path.exists(fn):
            os.remove(fn)

# -------------------------
# OSRM Table API (async)
# -------------------------
def coords_to_str(coord):
    return f"{coord[0]},{coord[1]}"

async def call_table_api(session: aiohttp.ClientSession, nodes_coords, s_count, d_count):
    coords = ";".join(coords_to_str(n) for n in nodes_coords)
    sources_param = ";".join(str(i) for i in range(0, s_count))
    dests_param = ";".join(str(i) for i in range(s_count, s_count + d_count))
    url = f"{OSRM_BASE}{TABLE_PATH}{coords}?annotations=distance,duration&sources={sources_param}&destinations={dests_param}"

    backoff = BACKOFF
    for attempt in range(1, RETRY + 1):
        try:
            async with session.get(url, timeout=PER_REQUEST_TIMEOUT) as resp:
                # we will capture the JSON even on non-200 for logging
                text = await resp.text()
                if resp.status != 200:
                    # raise to trigger retry logic, but include body for logs
                    raise aiohttp.ClientResponseError(
                        request_info=resp.request_info, history=resp.history,
                        status=resp.status, message=f"HTTP {resp.status}: {text}", headers=resp.headers
                    )
                return json.loads(text)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == RETRY:
                # bubble up last error
                raise
            await asyncio.sleep(backoff)
            backoff *= 2

# -------------------------
# Worker: per chunk-pair with logging & safe write
# -------------------------
async def _worker_request(session, sem, s_start, d_start, src_nodes, dst_nodes, pair_positions, pair_indices, results, cache, uid):
    nodes = src_nodes + dst_nodes
    s_count = len(src_nodes)
    d_count = len(dst_nodes)

    async with sem:
        try:
            data = await call_table_api(session, nodes, s_count, d_count)
        except Exception as exc:
            # log failure
            logpath = os.path.join(LOG_DIR, f"chunk_fail_{uid}.json")
            with open(logpath, "w", encoding="utf-8") as fh:
                fh.write(json.dumps({
                    "error": str(exc),
                    "s_start": s_start, "d_start": d_start,
                    "s_count": s_count, "d_count": d_count,
                    "src_nodes": src_nodes[:3], "dst_nodes": dst_nodes[:3]
                }, indent=2))
            # mark these as None in cache/results
            for pos in pair_positions:
                si, di = pair_indices[pos]
                key = f"{si}:{di}"
                cache[key] = (None, None)
                results[pos] = (None, None)
            return

    dists_mat = data.get("distances")
    times_mat = data.get("durations")

    # if response looks bad, log content
    if dists_mat is None and times_mat is None:
        logpath = os.path.join(LOG_DIR, f"chunk_empty_{uid}.json")
        with open(logpath, "w", encoding="utf-8") as fh:
            fh.write(json.dumps({
                "response": data,
                "s_start": s_start, "d_start": d_start, "s_count": s_count, "d_count": d_count
            }, indent=2))

    for pos in pair_positions:
        si, di = pair_indices[pos]
        local_si = si - s_start
        local_di = di - d_start
        dist_val = None
        time_val = None
        try:
            if dists_mat is not None and dists_mat[local_si] is not None and dists_mat[local_si][local_di] is not None:
                dist_val = dists_mat[local_si][local_di] / 1000.0
            if times_mat is not None and times_mat[local_si] is not None and times_mat[local_si][local_di] is not None:
                time_val = times_mat[local_si][local_di] / 3600.0
        except Exception:
            dist_val = None
            time_val = None

        key = f"{si}:{di}"
        cache[key] = (dist_val, time_val)
        results[pos] = (dist_val, time_val)

# -------------------------
# Build chunks & mapping
# -------------------------
def build_chunks_and_map(global_nodes, pair_indices, max_nodes):
    total_nodes = len(global_nodes)
    node_chunks = []
    for start in range(0, total_nodes, max_nodes):
        end = min(start + max_nodes, total_nodes)
        node_chunks.append((start, end))

    index_to_chunk = {}
    for chunk_id, (start, end) in enumerate(node_chunks):
        for idx in range(start, end):
            index_to_chunk[idx] = chunk_id

    chunk_pair_map = {}
    for pos, (si, di) in enumerate(pair_indices):
        sc = index_to_chunk[si]
        dc = index_to_chunk[di]
        key = (sc, dc)
        chunk_pair_map.setdefault(key, []).append(pos)

    return node_chunks, chunk_pair_map

# -------------------------
# Async runner with logging
# -------------------------
async def perform_all_requests(chunk_pair_map, node_chunks, global_nodes, pair_indices, results, cache, concurrency):
    sem = asyncio.Semaphore(concurrency)
    timeout = aiohttp.ClientTimeout(total=None)
    connector = aiohttp.TCPConnector(limit_per_host=concurrency, force_close=False)
    headers = {"User-Agent": "async-batch-osrm/1.0"}

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
        tasks = []
        uid = 0
        for (sc, dc), positions in chunk_pair_map.items():
            s_start, s_end = node_chunks[sc]
            d_start, d_end = node_chunks[dc]
            src_nodes = global_nodes[s_start:s_end]
            dst_nodes = global_nodes[d_start:d_end]
            task = asyncio.create_task(
                _worker_request(session, sem, s_start, d_start, src_nodes, dst_nodes, positions, pair_indices, results, cache, uid)
            )
            tasks.append(task)
            uid += 1

        progress = tqdm(total=len(tasks), desc="Matrix requests", unit="req")
        for coro in asyncio.as_completed(tasks):
            try:
                await coro
            except Exception as e:
                # worker already logs; continue
                pass
            progress.update(1)
        progress.close()

# -------------------------
# Fallback synchronous per-pair
# -------------------------
def call_table_single_pair_sync(src, dst, timeout=30):
    nodes = [src, dst]
    coords = ";".join(f"{lon},{lat}" for lon,lat in nodes)
    url = f"{OSRM_BASE}{TABLE_PATH}{coords}?annotations=distance,duration&sources=0&destinations=1"
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    resp = r.json()
    d = resp.get("distances")
    t = resp.get("durations")
    dist_km = None
    dur_hr = None
    if d and d[0] is not None and d[0][0] is not None:
        dist_km = d[0][0] / 1000.0
    if t and t[0] is not None and t[0][0] is not None:
        dur_hr = t[0][0] / 3600.0
    return dist_km, dur_hr

# -------------------------
# Main processing function
# -------------------------
def process_with_async_table(input_file, output_prefix, max_nodes=MAX_NODES_PER_REQUEST, concurrency=MAX_CONCURRENT_REQUESTS, clear_cache=False):
    df = pd.read_excel(input_file)
    df = df.copy()

    coord_cols = ['source longitude','source latitude','destination longitude','destination latitude']
    df[coord_cols] = df[coord_cols].replace({'(blank)': None, '': None, 'NA': None, 'N/A': None})
    for c in coord_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    bad_mask = df[coord_cols].isna().any(axis=1)
    num_bad = bad_mask.sum()
    total = len(df)
    print(f"Coordinate parsing: {total - num_bad} valid rows, {num_bad} invalid rows.")
    if num_bad > 0:
        df[bad_mask].to_csv("bad_coordinate_rows.csv", index=False)
        print("Wrote invalid rows to bad_coordinate_rows.csv")

    valid_df = df[~bad_mask].copy().reset_index(drop=False)

    # build nodes/pairs
    pairs = []
    unique_nodes_map = {}
    global_nodes = []

    def add_node(coord):
        if coord not in unique_nodes_map:
            unique_nodes_map[coord] = len(global_nodes)
            global_nodes.append(coord)
        return unique_nodes_map[coord]

    for _, row in valid_df.iterrows():
        src = (row['source longitude'], row['source latitude'])
        dst = (row['destination longitude'], row['destination latitude'])
        pairs.append((src, dst))
        add_node(src)
        add_node(dst)

    pair_indices = [(unique_nodes_map[s], unique_nodes_map[d]) for s, d in pairs]
    total_valid_rows = len(pair_indices)
    print(f"Processing {total_valid_rows} valid rows. Unique nodes: {len(global_nodes)}")

    # cache
    if clear_cache:
        clear_cache_file(CACHE_FILE)
        print("Cache cleared.")
    cache = load_cache_to_dict(CACHE_FILE)

    results = [None] * total_valid_rows
    missing_positions = []
    hits = 0
    for i, (si, di) in enumerate(pair_indices):
        key = f"{si}:{di}"
        if key in cache:
            results[i] = cache[key]
            hits += 1
        else:
            missing_positions.append(i)
    print(f"Cache hits: {hits}, missing: {len(missing_positions)}")

    if not missing_positions:
        print("All results from cache — writing output.")
        df['distance_km'] = None
        df['duration_hr'] = None
        for i, (dist, dur) in enumerate(results):
            orig_idx = int(valid_df.loc[i, 'index'])
            df.at[orig_idx, 'distance_km'] = dist
            df.at[orig_idx, 'duration_hr'] = dur
        out_file = f"{output_prefix}_with_results.xlsx"
        df.to_excel(out_file, index=False)
        print(f"Saved: {out_file}")
        return

    # chunk mapping
    node_chunks, full_chunk_pair_map = build_chunks_and_map(global_nodes, pair_indices, max_nodes)

    # reduce to needed chunk pairs
    needed_chunk_pair_map = {}
    for (sc, dc), positions in full_chunk_pair_map.items():
        needed_positions = [p for p in positions if p in missing_positions]
        if needed_positions:
            needed_chunk_pair_map[(sc, dc)] = needed_positions

    print(f"Chunk pairs to request: {len(needed_chunk_pair_map)} (max_nodes={max_nodes})")

    # run async
    asyncio.run(perform_all_requests(needed_chunk_pair_map, node_chunks, global_nodes, pair_indices, results, cache, concurrency))

    # Save cache
    dump_cache_from_dict(CACHE_FILE, cache)

    # Recompute missing positions after matrix phase
    still_missing = [i for i, r in enumerate(results) if r is None or (r[0] is None and r[1] is None)]
    print(f"After matrix phase: still missing {len(still_missing)} of {total_valid_rows}")

    # Fallback: try single-pair synchronous requests for still-missing
    filled_by_fallback = 0
    if still_missing:
        print("Starting synchronous fallback for missing pairs (this is slower).")
        for pos in tqdm(still_missing, desc="Fallback pairs"):
            si, di = pair_indices[pos]
            src = global_nodes[si]
            dst = global_nodes[di]
            key = f"{si}:{di}"
            try:
                dist, dur = call_table_single_pair_sync(src, dst, timeout=PER_REQUEST_TIMEOUT)
                cache[key] = (dist, dur)
                results[pos] = (dist, dur)
                if dist is not None or dur is not None:
                    filled_by_fallback += 1
            except Exception as e:
                # log the exception
                logpath = os.path.join(LOG_DIR, f"fallback_fail_{pos}.json")
                with open(logpath, "w", encoding="utf-8") as fh:
                    fh.write(json.dumps({"pos": pos, "si": si, "di": di, "src": src, "dst": dst, "error": str(e)}, indent=2))
                cache[key] = (None, None)
                results[pos] = (None, None)
            time.sleep(FALLBACK_SLEEP)

    # Save cache again
    dump_cache_from_dict(CACHE_FILE, cache)

    # Merge back to dataframe
    df['distance_km'] = None
    df['duration_hr'] = None
    for i, r in enumerate(results):
        orig_idx = int(valid_df.loc[i, 'index'])
        if r is not None:
            df.at[orig_idx, 'distance_km'] = r[0]
            df.at[orig_idx, 'duration_hr'] = r[1]
        else:
            df.at[orig_idx, 'distance_km'] = None
            df.at[orig_idx, 'duration_hr'] = None

    out_file = f"{output_prefix}_with_results.xlsx"
    df.to_excel(out_file, index=False)

    print(f"Saved: {out_file}")
    print(f"Summary: cache_hits={hits}, matrix_requested={len(needed_chunk_pair_map)}, filled_by_fallback={filled_by_fallback}, still_missing={len([i for i,r in enumerate(results) if r is None or (r[0] is None and r[1] is None)])}")

# -------------------------
# CLI / run
# -------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", default="coordinates.xlsx")
    parser.add_argument("--output-prefix", "-o", default="coordinates_with_results_async")
    parser.add_argument("--max-nodes", type=int, default=MAX_NODES_PER_REQUEST)
    parser.add_argument("--concurrency", type=int, default=MAX_CONCURRENT_REQUESTS)
    parser.add_argument("--clear-cache", action="store_true")
    args = parser.parse_args()

    process_with_async_table(args.input, args.output_prefix, max_nodes=args.max_nodes, concurrency=args.concurrency, clear_cache=args.clear_cache)