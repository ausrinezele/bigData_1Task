import csv
import os
import logging
from multiprocessing import Pool, cpu_count
from typing import Generator, Iterator

MMSI_COL = "MMSI"

# naudojam set, o ne list, nes set bus O(1), o list O(n)
# blogi kodai
INVALID_MMSI_EXACT: set[str] = {
    "000000000",
    "111111111",
    "123456789",
    "999999999",
    "012345678",
}

MMSI_MIN_LEN = 9
MMSI_MAX_LEN = 9

# reject clearly nonsensical values.
MMSI_NUMERIC_MIN = 100_000_000   # test transmitter or bad data
MMSI_NUMERIC_MAX = 999_999_999   # impossible (9-digit ceiling)

# Default chunk size sent to each worker (rows, not bytes).
# 50 000 rows ~ 15–25 MB RAM per chunk 
DEFAULT_CHUNK_SIZE = 50_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# checks are ordered from cheapest to most expensive

def is_valid_mmsi(mmsi_raw: str) -> bool:
    mmsi = mmsi_raw.strip()

    if not mmsi:
        return False

    if mmsi in INVALID_MMSI_EXACT:
        return False

    if not mmsi.isdigit():                 # 'N/A', 'Unknown'
        return False

    if len(mmsi) != MMSI_MIN_LEN:          # exactly 9 digits
        return False

    value = int(mmsi)
    if not (MMSI_NUMERIC_MIN <= value <= MMSI_NUMERIC_MAX):
        return False

    return True


def _row_generator(filepath: str) -> Generator[dict, None, None]:
    with open(filepath, newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)

        # Normalise header names: strip BOM, whitespace, invisible chars.
        reader.fieldnames = [
            name.lstrip("\ufeff").strip() for name in (reader.fieldnames or [])
        ]

        for lineno, row in enumerate(reader, start=2):
            if not row:
                continue

            mmsi_raw = row.get(MMSI_COL, "")

            if not is_valid_mmsi(mmsi_raw):
                # išspausdins blogų eilučių skaičių (bet bendrą skaičių tik)
                continue

            yield row # yield suspends the function, gives back one value, and resumes later

# This function takes the row-by-row stream from _row_generator and groups it into lists of chunk_size rows.
def _chunked(
    iterator: Iterator[dict],
    chunk_size: int,
) -> Generator[list[dict], None, None]:

    chunk: list[dict] = []
    for row in iterator:
        chunk.append(row)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = [] # Python's garbage collector handle memory cleanly

    if chunk:             
        yield chunk


# separate Python processes (not threads)
def stream_to_workers(
    filepath: str,
    worker_fn,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    n_workers: int | None = None,
    worker_kwargs: dict | None = None,
) -> list:

    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"AIS file not found: {filepath!r}")

    n_workers = n_workers or max(1, cpu_count() - 1)
    worker_kwargs = worker_kwargs or {}

    log.info(
        "Starting partitioner | file=%s | chunk_size=%d | workers=%d",
        filepath, chunk_size, n_workers,
    )

    rows_dispatched = 0
    chunks_dispatched = 0
    results: list = []

    # imap_unordered streams results back as soon as any worker finishes
    with Pool(processes=n_workers) as pool:
        row_stream   = _row_generator(filepath)
        chunk_stream = _chunked(row_stream, chunk_size)

        def _make_call(chunk):
            return (chunk, worker_kwargs)

        # chunksize=1 here means imap sends individual tasks 
        futures = pool.imap_unordered(
            _worker_wrapper(worker_fn),
            ({"chunk": c, **worker_kwargs} for c in chunk_stream),
            chunksize=1,
        )

        for result in futures:
            results.append(result)
            chunks_dispatched += 1
            rows_dispatched += result.get("rows_processed", 0) \
                if isinstance(result, dict) else 0

            if chunks_dispatched % 100 == 0:
                log.info(
                    "Progress: %d chunks dispatched (~%d rows)",
                    chunks_dispatched, rows_dispatched,
                )

    log.info(
        "Partitioner done | chunks=%d | rows_dispatched≈%d",
        chunks_dispatched, rows_dispatched,
    )
    return results


# handles the unpacking so worker_fn stays clean)

class _worker_wrapper:
    def __init__(self, fn):
        self.fn = fn

    def __call__(self, kwargs: dict):
        chunk = kwargs.pop("chunk")
        return self.fn(chunk, **kwargs)


# for smoke-testing
def _example_worker(chunk: list[dict]) -> dict:
    """
    Trivial worker: count rows and collect unique MMSI values seen.
    Replace this with real anomaly-detection logic in later tasks.
    """
    unique_mmsi = {row[MMSI_COL] for row in chunk}
    return {
        "rows_processed": len(chunk),
        "unique_mmsi_count": len(unique_mmsi),
    }



if __name__ == "__main__":
    import sys
    import time

    if len(sys.argv) < 2:
        print("Usage: python ais_partitioner.py <path/to/aisdk-YYYY-MM-DD.csv>")
        sys.exit(1)

    target_file = sys.argv[1]
    chunk_sz    = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_CHUNK_SIZE

    t0 = time.perf_counter()
    all_results = stream_to_workers(
        filepath   = target_file,
        worker_fn  = _example_worker,
        chunk_size = chunk_sz,
    )
    elapsed = time.perf_counter() - t0

    total_rows  = sum(r["rows_processed"]    for r in all_results)
    total_mmsi  = sum(r["unique_mmsi_count"] for r in all_results)

    print(f"\n{'='*55}")
    print(f"  File          : {target_file}")
    print(f"  Chunk size    : {chunk_sz:,} rows")
    print(f"  Chunks done   : {len(all_results):,}")
    print(f"  Rows processed: {total_rows:,}  (after dirty-MMSI filter)")
    print(f"  MMSI (non-unique across chunks): {total_mmsi:,}")
    print(f"  Wall time     : {elapsed:.2f}s")
    print(f"{'='*55}\n")