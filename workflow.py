"""Bound uploads by verification/removal barriers rather than an unbounded queue."""

from concurrent.futures import ThreadPoolExecutor
import time
from pathlib import Path
import hashlib
import json
import threading


def batches(rows, size):
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def run_batches(
    rows,
    *,
    task,
    drain,
    errors,
    waiting,
    workers,
    batch_size,
    after_batch,
    stop_event,
    timeout,
    pause,
):
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for group in batches(rows, batch_size):
                if stop_event.is_set():
                    break
                # Submit only this batch. Google processing must not let uploads
                # accumulate across the library while verification falls behind.
                for future in [pool.submit(task, row) for row in group]:
                    future.result()
                end = time.monotonic() + timeout
                drain()
                delay = pause
                while (
                    waiting()
                    and not errors
                    and not stop_event.is_set()
                    and time.monotonic() < end
                ):
                    time.sleep(min(delay, max(0, end - time.monotonic())))
                    drain()
                    delay = min(20, delay * 1.5)
                if errors or waiting():
                    stop_event.set()
                    break
                if after_batch:
                    after_batch([row[0] for row in group])
    except BaseException:
        stop_event.set()
        raise


# In-process workers share one atomic, account-bound diagnostic ledger.
_failure_lock = threading.RLock()


def record_item_failure(worker, root, state, error, event, *, key, folder):
    """Defer only known failures before source removal; never certify completion."""
    from converter import atomic_json, digest
    from removal import local_backup

    if state.get("original_key") != key or state.get("account") != worker.account:
        raise RuntimeError("Failed-item journal identity/account mismatch")
    message, stage = str(error), state.get("stage")
    transport = {"timed out", "TypeError: Failed to fetch",
                 "Error: Request deadline exceeded; reconcile writes before retrying",
                 "HTTP Error 500: Internal Server Error", "HTTP Error 502: Bad Gateway",
                 "HTTP Error 503: Service Unavailable", "HTTP Error 504: Gateway Timeout"}
    if stage == "committing" and message in transport and getattr(worker, "continue_on_item_error", False):
        worker.write(state, commit_transport_error=message)
        message = "Commit outcome unknown; reconcile using hash"
    worker.write(state, error=message)
    eligible = False
    backup_required = False
    if getattr(worker, "continue_on_item_error", False) and not state.get("permanent_delete_started"):
        if message == "Original source is unavailable":
            eligible = not stage and not any(state.get(k) for k in ("source", "original_info", "replacement_key"))
        elif message in ("Capture date repair did not verify", "Video capture date/zone mismatch"):
            eligible = stage == "uploaded" and bool(state.get("replacement_key"))
            backup_required = True
        elif message == "Commit outcome unknown; reconcile using hash" or message.startswith("Previous upload commit is ambiguous and its hash is not visible yet."):
            eligible = stage == "committing" and not state.get("replacement_key")
            backup_required = True
        elif message in ("TypeError: Failed to fetch", "timed out", "Error: Request deadline exceeded; reconcile writes before retrying", "HTTP Error 500: Internal Server Error", "HTTP Error 502: Bad Gateway", "HTTP Error 503: Service Unavailable", "HTTP Error 504: Gateway Timeout"):
            eligible = stage in (None, "downloaded", "converted") and not state.get("replacement_key")
            backup_required = bool(state.get("original_sha256") or stage)
            # A partial or unjournaled file needs inspection, not silent deferral.
            if state.get("source") and Path(state["source"]).exists():
                backup_required = True
    if eligible:
        if backup_required:
            local_backup(state)
            if stage in ("committing", "uploaded"):
                expected = state.get("avif_sha256") or state.get("encoded_sha256")
                if not expected or digest(Path(state["output"])) != expected:
                    raise RuntimeError("Deferred output checksum mismatch; inspection required")
        root = Path(root)
        if not hasattr(worker, "_failure_sources"):
            inventory = getattr(worker, "inventory", None) or getattr(worker, "inv", None)
            if inventory is None:
                inventory = json.loads((root / "inventory-api.json").read_text())
            if inventory["account"] != worker.account:
                raise RuntimeError("Deferred inventory account mismatch")
            worker._failure_sources = frozenset(r[0] for r in inventory["items"])
        if key not in worker._failure_sources:
            raise RuntimeError("Deferred source differs from bound inventory")
        journal = root / folder / key / "state.json"
        if json.loads(journal.read_text()) != json.loads(json.dumps(state)):
            raise RuntimeError("Deferred journal changed during inspection")
        with _failure_lock:
            path = root / "deferred-items.json"
            ledger = json.loads(path.read_text()) if path.exists() else {"account": worker.account, "items": {}}
            if ledger.get("account") != worker.account or not isinstance(ledger.get("items"), dict):
                raise RuntimeError("Deferred ledger account/schema mismatch")
            ledger["items"][key] = dict(reason=message, stage=stage, recorded_at=time.time(),
                                        journal_sha256=hashlib.sha256(journal.read_bytes()).hexdigest(),
                                        backup_verified=backup_required, source_removal_attempted=False)
            atomic_json(path, ledger)
        worker.deferred.append(key)
        worker.log("item_deferred", key=key, error=message, stage=stage,
                   backup_verified=backup_required, selection_unchanged=True)
        return True
    worker.errors.append(key)
    worker.log(event, key=key, error=message)
    return False


class SourceUnavailableError(RuntimeError):
    """An individual original cannot currently be read; no source was removed."""


class VideoPrefetch:
    """Bounded, read-only lookahead. Only foreground workers encode or upload."""

    def __init__(self, rows, prepare, eligible, estimate, stop_event, directory,
                 *, size=2, max_bytes=2 * 1024 ** 3):
        if type(size) is not int or not 1 <= size <= 4:
            raise ValueError("Video prefetch must be between 1 and 4")
        self.rows = iter(rows)
        self.prepare_source = prepare
        self.eligible = eligible
        self.estimate = estimate
        self.stop_event = stop_event
        self.directory = Path(directory)
        self.size, self.max_bytes = size, max_bytes
        self.futures = {}
        self.reserved = 0
        self.lock = threading.RLock()
        self.closed = False
        self.failure = None

    def __enter__(self):
        self.pool = ThreadPoolExecutor(max_workers=min(2, self.size))
        try:
            with self.lock:
                self._fill()
        except BaseException:
            self.pool.shutdown(wait=True, cancel_futures=True)
            raise
        return self

    @staticmethod
    def _item_error(error):
        from http.client import IncompleteRead
        from urllib.error import HTTPError, URLError

        if isinstance(error, HTTPError):
            return error.code in (429, 500, 502, 503, 504)
        if isinstance(error, URLError):
            return isinstance(error.reason, (TimeoutError, ConnectionError))
        return isinstance(error, (SourceUnavailableError, TimeoutError, ConnectionError, IncompleteRead)) or str(error) == "TypeError: Failed to fetch"

    def _prepare(self, row):
        from concurrent.futures import CancelledError

        if self.stop_event.is_set():
            raise CancelledError()
        try:
            return self.prepare_source(row)
        except Exception as error:
            if not self._item_error(error):
                with self.lock:
                    self.failure = self.failure or error
                    self.stop_event.set()
            raise

    def _fill(self):
        import shutil

        while not self.closed and not self.stop_event.is_set() and len(self.futures) < self.size:
            row = next(self.rows, None)
            if row is None:
                return
            if not self.eligible(row):
                continue
            # Export temporary files and their atomic backup can coexist.
            # Oversized/unknown sources still run normally in the foreground.
            cost = self.estimate(row)
            if type(cost) is not int or cost <= 0:
                continue
            cost *= 2
            available = shutil.disk_usage(self.directory).free - 1024 ** 3
            if self.reserved + cost > min(self.max_bytes, available):
                continue
            key = row[0]
            if key in self.futures:
                raise RuntimeError("Duplicate source in video prefetch selection")
            self.reserved += cost
            self.futures[key] = (self.pool.submit(self._prepare, row), cost)

    def check(self):
        if self.failure is not None:
            raise self.failure

    def prepare(self, row):
        with self.lock:
            pair = self.futures.pop(row[0], None)
        try:
            self.check()
            return pair[0].result() if pair else self.prepare_source(row)
        finally:
            with self.lock:
                if pair:
                    self.reserved -= pair[1]
                self._fill()

    def __exit__(self, kind, error, traceback):
        with self.lock:
            self.closed = True
            for future, _ in self.futures.values():
                future.cancel()
        # Finish active atomic local backups before releasing the migration lock.
        self.pool.shutdown(wait=True, cancel_futures=True)
        if kind is None:
            self.check()
