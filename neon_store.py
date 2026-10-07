"""Neon-backed storage for Black Server My System.

Two Neon services are used and nothing else:

  * **Object Storage** (S3-compatible) stores the file *bytes*.
  * **Postgres** stores the file *metadata* (key, name, size, type, date, user).

No AWS S3, no Cloudflare R2, no local/server disk is used for storage when this
backend is active: uploaded bytes go straight to the bucket and are removed from
the bucket on delete.  The only disk usage is an in-memory spool
(``tempfile.SpooledTemporaryFile``) that buffers one multipart chunk while it is
being streamed to the bucket; it never persists between requests.

Configuration comes from the environment (a local ``.env`` file is also read, but
real environment variables always win).  Secrets are never stored in code:

  =============================  ====================================================
  ``AWS_ENDPOINT_URL_S3``        Object Storage endpoint (falls back to ``AWS_ENDPOINT_URL``)
  ``AWS_REGION``                 region, e.g. ``us-east-1``
  ``AWS_ACCESS_KEY_ID``          object storage key id
  ``AWS_SECRET_ACCESS_KEY``      object storage secret
  ``BUCKET_NAME``                bucket that holds the files
  ``DATABASE_URL``               Neon Postgres connection string
  ``BS_PRESIGN_TTL``             presigned URL lifetime in seconds (default 3600)
  ``BS_USER``                    fallback owner name when the request has no user
  =============================  ====================================================

The client is created with ``addressing_style="path"`` (boto3's equivalent of the
``forcePathStyle`` flag used by Neon's JavaScript examples), which is required by
Neon Object Storage.
"""

from __future__ import annotations

import io
import mimetypes
import os
import tempfile
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

# --------------------------------------------------------------------------
# configuration
# --------------------------------------------------------------------------

#: fallback expiry for ``generate_presigned_url`` links (1 hour)
DEFAULT_PRESIGN_TTL = 3600
#: multipart spool threshold: bytes kept in RAM before spilling to a temp file
SPOOL_MAX_MEMORY = 8 * 1024 * 1024
#: rows returned by :meth:`NeonStore.search`
SEARCH_LIMIT = 300


def load_dotenv(path: Optional[Path] = None) -> None:
    """Load ``KEY=VALUE`` pairs from a ``.env`` file without overriding the env.

    Real environment variables (e.g. the ones Render injects) always win; this
    only fills in what is missing so a local checkout works out of the box.
    """
    env_path = path or (Path(__file__).resolve().parent / ".env")
    try:
        raw = env_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.lower().startswith("export "):
            line = line[7:].lstrip()
        key, _, value = line.partition("=")
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ[key] = value


def _setting(*names: str) -> str:
    """First non-empty environment value among *names*."""
    for name in names:
        value = (os.environ.get(name) or "").strip()
        if value:
            return value
    return ""


def key_from_url(raw_path: str) -> str:
    """Percent-decoded, traversal-free storage key for a URL path.

    ``/docs/a%20b.txt`` -> ``docs/a b.txt``; ``..`` segments are dropped so a
    crafted URL can never escape the bucket prefix.
    """
    path = urllib.parse.unquote(raw_path or "/").replace("\\", "/")
    return "/".join(
        part for part in path.split("/") if part not in ("", ".", "..")
    )


def guess_content_type(name: str) -> str:
    """Best-effort MIME type for *name* (always returns something usable)."""
    guessed, _ = mimetypes.guess_type(name)
    return guessed or "application/octet-stream"


@dataclass
class Entry:
    """A file or folder, mirroring the attributes the listing UI expects."""

    name: str
    is_dir: bool = False
    size: int = 0
    mtime: float = 0.0
    content_type: str = "application/octet-stream"
    user: str = ""
    key: str = ""

    @property
    def suffix(self) -> str:
        return Path(self.name).suffix

    @property
    def is_file(self) -> bool:
        return not self.is_dir


@dataclass
class StatLike:
    """``os.stat_result`` lookalike so existing renderers keep working."""

    st_size: int = 0
    st_mtime: float = 0.0
    st_mode: int = 0o644


# --------------------------------------------------------------------------
# storage backend
# --------------------------------------------------------------------------


class NeonStore:
    """Neon Object Storage (bytes) + Neon Postgres (metadata)."""

    def __init__(
        self,
        *,
        endpoint: str,
        region: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        database_url: str,
        presign_ttl: int = DEFAULT_PRESIGN_TTL,
        default_user: str = "anonymous",
    ) -> None:
        import boto3  # imported lazily so the app still runs without the extras
        from botocore.config import Config

        self.bucket = bucket
        self.database_url = database_url
        self.presign_ttl = presign_ttl
        self.default_user = default_user
        # path-style addressing (Neon's forcePathStyle equivalent)
        self._s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 3, "mode": "standard"},
            ),
        )
        self._db_lock = threading.Lock()
        self._db = None  # psycopg connection, opened on first use
        self._ready = False

    # -- infrastructure --------------------------------------------------
    @property
    def s3(self):
        """The configured boto3 S3 client."""
        return self._s3

    def _conn(self):
        """Return a live Postgres connection, reconnecting when it dropped."""
        import psycopg

        with self._db_lock:
            if self._db is not None and self._db.closed == 0:
                return self._db
            # Neon wants TLS; keep sslmode=require unless the URL says otherwise
            dsn = self.database_url
            if "sslmode=" not in dsn:
                dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
            self._db = psycopg.connect(dsn, connect_timeout=15)
            return self._db

    def _cursor(self):
        return self._conn().cursor()

    def init_schema(self) -> None:
        """Create the metadata table / indexes if they do not exist yet."""
        if self._ready:
            return
        with self._db_lock:
            conn = self._conn()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS files (
                        id           BIGSERIAL PRIMARY KEY,
                        key          TEXT        NOT NULL UNIQUE,
                        name         TEXT        NOT NULL,
                        parent       TEXT        NOT NULL DEFAULT '',
                        is_dir       BOOLEAN     NOT NULL DEFAULT FALSE,
                        size         BIGINT      NOT NULL DEFAULT 0,
                        content_type TEXT        NOT NULL DEFAULT 'application/octet-stream',
                        object_key   TEXT,
                        created_by   TEXT        NOT NULL DEFAULT 'anonymous',
                        created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS files_parent_idx ON files (parent)")
                cur.execute("CREATE INDEX IF NOT EXISTS files_name_idx ON files (lower(name))")
            conn.commit()
            self._ready = True

    def close(self) -> None:
        """Release the Postgres connection (used on shutdown / in tests)."""
        with self._db_lock:
            if self._db is not None:
                try:
                    self._db.close()
                except Exception:  # noqa: BLE001 - best effort teardown
                    pass
                self._db = None

    # -- key helpers -----------------------------------------------------
    @staticmethod
    def normalise(rel: str) -> str:
        """Turn a UI path (``/a/b``) into an object key prefix (``a/b/``)."""
        parts = [p for p in str(rel or "/").replace("\\", "/").split("/") if p and p != "."]
        parts = [p for p in parts if p != ".."]
        return "/".join(parts)

    def child_key(self, rel: str, name: str) -> str:
        """Object key for *name* inside folder *rel* (``""`` for the root)."""
        prefix = self.normalise(rel)
        key = f"{prefix}/{name}" if prefix else str(name)
        return self.normalise(key)

    # -- object storage --------------------------------------------------
    def put_object(self, key: str, body, *, content_type: str) -> None:
        """Stream *body* (bytes or file-like) into the bucket under *key*."""
        extra = {}
        if content_type:
            extra["ContentType"] = content_type
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=body, **extra)

    def delete_object(self, key: str) -> None:
        """Remove one object; missing objects are not an error."""
        self._s3.delete_object(Bucket=self.bucket, Key=key)

    def download_url(self, key: str, *, filename: str = "", inline: bool = False,
                     expires: Optional[int] = None) -> str:
        """Temporary download link via ``generate_presigned_url``."""
        params = {"Bucket": self.bucket, "Key": key}
        if filename:
            quoted = urllib.parse.quote(filename)
            mode = "inline" if inline else "attachment"
            params["ResponseContentDisposition"] = f"{mode}; filename=\"{quoted}\""
        return self._s3.generate_presigned_url(
            "get_object", Params=params, ExpiresIn=int(expires or self.presign_ttl)
        )

    def get_object(self, key: str):
        """Open an object for streaming (``StreamingBody``)."""
        return self._s3.get_object(Bucket=self.bucket, Key=key)

    def object_exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self._s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    # -- metadata --------------------------------------------------------
    def record_file(self, key: str, *, name: str, size: int, content_type: str,
                    user: str) -> None:
        """Insert (or refresh) the metadata row for an uploaded object."""
        self.init_schema()
        parent = key.rsplit("/", 1)[0] if "/" in key else ""
        with self._conn().cursor() as cur:
            cur.execute(
                """
                INSERT INTO files (key, name, parent, is_dir, size, content_type,
                                   object_key, created_by)
                VALUES (%s, %s, %s, FALSE, %s, %s, %s, %s)
                ON CONFLICT (key) DO UPDATE
                   SET name = EXCLUDED.name,
                       parent = EXCLUDED.parent,
                       size = EXCLUDED.size,
                       content_type = EXCLUDED.content_type,
                       object_key = EXCLUDED.object_key,
                       updated_at = now()
                """,
                (key, name, parent, size, content_type, key, user or self.default_user),
            )
        self._conn().commit()

    def record_dir(self, rel: str, *, user: str = "") -> bool:
        """Create a folder row. Returns False when it already existed."""
        name = self.normalise(rel).rsplit("/", 1)[-1]
        if not name:
            return False
        key = self.normalise(rel) + "/"
        self.init_schema()
        parent = key[:-1].rsplit("/", 1)[0] if "/" in key[:-1] else ""
        with self._conn().cursor() as cur:
            cur.execute(
                """
                INSERT INTO files (key, name, parent, is_dir, object_key, created_by)
                VALUES (%s, %s, %s, TRUE, NULL, %s)
                ON CONFLICT (key) DO NOTHING
                """,
                (key, name, parent, user or self.default_user),
            )
            created = cur.rowcount > 0
        self._conn().commit()
        return created

    def delete_entry(self, rel: str) -> tuple[bool, bool]:
        """Delete one file or a whole folder subtree.

        Returns ``(found, was_dir)``.
        """
        key = self.normalise(rel)
        if not key:
            return False, False  # never allow removing the root
        self.init_schema()
        with self._conn().cursor() as cur:
            cur.execute("SELECT is_dir FROM files WHERE key = %s", (key,))
            row = cur.fetchone()
            if row is None:
                # maybe it exists only as an object (uploaded before the row)
                if self.object_exists(key):
                    self.delete_object(key)
                    self._conn().commit()
                    return True, False
                return False, False
            is_dir = bool(row[0])
            if is_dir:
                cur.execute(
                    "SELECT object_key FROM files WHERE key LIKE %s AND object_key IS NOT NULL",
                    (key + "/%",),
                )
                keys = [r[0] for r in cur.fetchall()]
                cur.execute("DELETE FROM files WHERE key = %s OR key LIKE %s", (key, key + "/%"))
            else:
                keys = [row_key for row_key in [key]]
                cur.execute("DELETE FROM files WHERE key = %s", (key,))
        self._conn().commit()
        for object_key in keys:
            try:
                self.delete_object(object_key)
            except Exception:  # noqa: BLE001 - metadata is already gone
                pass
        return True, is_dir

    def entry(self, rel: str) -> Optional[Entry]:
        """Metadata for a single path, or ``None``."""
        key = self.normalise(rel)
        self.init_schema()
        with self._conn().cursor() as cur:
            cur.execute(
                """
                SELECT key, name, is_dir, size, content_type, created_by,
                       extract(epoch FROM created_at)
                FROM files WHERE key = %s
                """,
                (key if key else "/",),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return Entry(
            name=row[1], is_dir=bool(row[2]), size=int(row[3] or 0),
            mtime=float(row[6] or 0.0), content_type=row[4], user=row[5], key=row[0],
        )

    def list_dir(self, rel: str) -> list[Entry]:
        """Direct children of *rel*; folders first, then files (A→Z)."""
        prefix = self.normalise(rel)
        parent = prefix
        self.init_schema()
        with self._conn().cursor() as cur:
            cur.execute(
                """
                SELECT key, name, is_dir, size, content_type, created_by,
                       extract(epoch FROM created_at)
                FROM files
                WHERE parent = %s
                ORDER BY is_dir DESC, lower(name)
                """,
                (parent,),
            )
            rows = cur.fetchall()
        return [
            Entry(name=r[1], is_dir=bool(r[2]), size=int(r[3] or 0), mtime=float(r[6] or 0.0),
                  content_type=r[4], user=r[5], key=r[0])
            for r in rows
        ]

    def exists(self, rel: str) -> bool:
        key = self.normalise(rel)
        if not key:
            return True
        return self.entry(key) is not None

    def folder_stats(self, rel: str) -> tuple[int, int]:
        """``(file_count, total_bytes)`` for everything under *rel*."""
        prefix = self.normalise(rel)
        pattern = prefix + "/%" if prefix else "%"
        self.init_schema()
        with self._conn().cursor() as cur:
            cur.execute(
                "SELECT count(*), coalesce(sum(size), 0) FROM files "
                "WHERE is_dir = FALSE AND key LIKE %s",
                (pattern,),
            )
            row = cur.fetchone()
        return int(row[0] or 0), int(row[1] or 0)

    def tree(self) -> dict:
        """Nested folder tree for the move/copy destination picker."""
        self.init_schema()
        with self._conn().cursor() as cur:
            cur.execute(
                "SELECT key FROM files WHERE is_dir = TRUE ORDER BY lower(key)"
            )
            keys = [r[0] for r in cur.fetchall()]
        root = {"name": "/", "path": "/", "dirs": []}
        nodes = {"/": root}
        for key in keys:
            path = "/" + key.strip("/")
            if path == "/":
                continue
            node = nodes.get(path)
            if node is None:
                node = {"name": path.rsplit("/", 1)[-1], "path": path, "dirs": []}
                nodes[path] = node
            parent_path = path.rsplit("/", 1)[0] or "/"
            parent = nodes.setdefault(parent_path, {"name": parent_path, "path": parent_path,
                                                    "dirs": []})
            parent["dirs"].append(node)
        return root

    def search(self, query: str) -> list[dict]:
        """Case-insensitive name search across every stored path."""
        needle = (query or "").strip().lower()
        if not needle:
            return []
        self.init_schema()
        with self._conn().cursor() as cur:
            cur.execute(
                """
                SELECT key, name, is_dir FROM files
                WHERE lower(name) LIKE %s
                ORDER BY lower(name)
                LIMIT %s
                """,
                ("%" + needle.replace("%", r"\%").replace("_", r"\_") + "%", SEARCH_LIMIT),
            )
            rows = cur.fetchall()
        results = []
        for key, name, is_dir in rows:
            rel = key.strip("/")
            results.append({
                "name": name,
                "path": rel,
                "kind": 1 if is_dir else 2,
                "href": urllib.parse.quote(rel, safe="") + ("/" if is_dir else ""),
            })
        return results

    def sidebar_paths(self) -> list[str]:
        """Sorted folder paths used to render the left sidebar."""
        self.init_schema()
        with self._conn().cursor() as cur:
            cur.execute("SELECT key FROM files WHERE is_dir = TRUE ORDER BY lower(key)")
            return ["/" + r[0].strip("/") for r in cur.fetchall() if r[0].strip("/")]

    def walk_files(self, rel: str) -> Iterable[tuple[str, str]]:
        """Yield ``(archive_name, object_key)`` for every file under *rel*."""
        start = self.normalise(rel)
        pending = [start]
        seen = set()
        while pending:
            current = pending.pop(0)
            if current in seen:
                continue
            seen.add(current)
            for entry in self.list_dir("/" + current if current else "/"):
                if entry.is_dir:
                    pending.append(entry.key.strip("/"))
                else:
                    yield (entry.key, entry.key)


# --------------------------------------------------------------------------
# multipart sink
# --------------------------------------------------------------------------


class ObjectPart:
    """Write target for one multipart chunk; uploads on :meth:`close`.

    Bytes are buffered in a spooled temp file (RAM first, disk only for very large
    parts) so the bucket receives a complete object and memory stays flat.
    """

    def __init__(self, store: NeonStore, key: str, name: str, user: str) -> None:
        self._store = store
        self.key = key
        self.name = name
        self.user = user
        self.size = 0
        self._buf = tempfile.SpooledTemporaryFile(max_size=SPOOL_MAX_MEMORY)
        self._closed = False

    def write(self, data: bytes) -> int:
        if self._closed:
            raise ValueError("write after close")
        self.size += len(data)
        self._buf.write(data)
        return len(data)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._buf.seek(0)
            content_type = guess_content_type(self.name)
            self._store.put_object(self.key, self._buf, content_type=content_type)
            self._store.record_file(
                self.key, name=self.name, size=self.size,
                content_type=content_type, user=self.user,
            )
        finally:
            try:
                self._buf.close()
            except Exception:  # noqa: BLE001 - best effort cleanup
                pass

    def abort(self) -> None:
        """Discard the part without uploading (used when a multipart parse fails)."""
        self._closed = True
        try:
            self._buf.close()
        except Exception:  # noqa: BLE001
            pass


# --------------------------------------------------------------------------
# singleton
# --------------------------------------------------------------------------

_STORE: Optional[NeonStore] = None
_STORE_READY = False
_STORE_LOCK = threading.Lock()


def build_store() -> Optional[NeonStore]:
    """Create a :class:`NeonStore` from the environment, or ``None``.

    ``None`` means "Neon is not configured" and the caller keeps using the local
    development store.  A partially configured environment is reported loudly
    instead of silently falling back.

    ``BS_STORAGE`` overrides the detection: ``local`` forces the disk backend,
    ``neon`` requires a complete configuration (and fails without one).
    """
    load_dotenv()
    forced = _setting("BS_STORAGE").lower()
    if forced == "local":
        return None

    endpoint = _setting("AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL")
    region = _setting("AWS_REGION", "AWS_DEFAULT_REGION")
    access_key = _setting("AWS_ACCESS_KEY_ID")
    secret_key = _setting("AWS_SECRET_ACCESS_KEY")
    bucket = _setting("BUCKET_NAME")
    database_url = _setting("DATABASE_URL")

    missing = [
        name
        for name, value in (
            ("AWS_ENDPOINT_URL_S3/AWS_ENDPOINT_URL", endpoint),
            ("AWS_REGION", region),
            ("AWS_ACCESS_KEY_ID", access_key),
            ("AWS_SECRET_ACCESS_KEY", secret_key),
            ("BUCKET_NAME", bucket),
            ("DATABASE_URL", database_url),
        )
        if not value
    ]
    if missing:
        if forced == "neon":
            raise RuntimeError(
                "BS_STORAGE=neon requires a complete configuration; missing: "
                + ", ".join(missing)
            )
        if len(missing) < 6:
            raise RuntimeError(
                "Neon storage is partially configured; missing: " + ", ".join(missing)
            )
        return None  # nothing configured -> plain local mode

    try:
        ttl = int(_setting("BS_PRESIGN_TTL") or DEFAULT_PRESIGN_TTL)
    except ValueError:
        ttl = DEFAULT_PRESIGN_TTL
    store = NeonStore(
        endpoint=endpoint,
        region=region,
        access_key=access_key,
        secret_key=secret_key,
        bucket=bucket,
        database_url=database_url,
        presign_ttl=ttl,
        default_user=_setting("BS_USER") or "anonymous",
    )
    store.init_schema()
    return store


def get_store() -> Optional[NeonStore]:
    """Process-wide :class:`NeonStore`, created on first use."""
    global _STORE, _STORE_READY
    if _STORE_READY:
        return _STORE
    with _STORE_LOCK:
        if not _STORE_READY:
            _STORE = build_store()
            _STORE_READY = True
    return _STORE


def reset_store() -> None:
    """Drop the cached store (tests / reconfiguration)."""
    global _STORE, _STORE_READY
    with _STORE_LOCK:
        if _STORE is not None:
            _STORE.close()
        _STORE = None
        _STORE_READY = False