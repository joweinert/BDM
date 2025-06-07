"""
bdm_duck  ·  Tiny helper to open the exploitation-zone DuckDB stored on MinIO.

Usage
-----
import bdm_duck as bd

bd.con.sql("SELECT COUNT(*) FROM expo_db.ecb_exchange_rates").show()

df = bd.table("imf_indicators").df()   # quick Pandas
"""

from __future__ import annotations
import os, duckdb, urllib.parse, functools
from dotenv import load_dotenv

load_dotenv()  # load .env file if present

# -----------------------------
#  Config via env vars
# -----------------------------
if os.getenv("DOCKER_ENV", "false") == "true":
    _MINIO_ENDPOINT = "minio:9000"
else:
    _MINIO_ENDPOINT = "localhost:9000"
_BUCKET = os.getenv("EXPLOITATION_ZONE_BUCKET")
_USER = os.getenv("MINIO_FIRST_TEAR_ANALYST_USER")
_PASS = os.getenv("MINIO_FIRST_TEAR_ANALYST_PASSWORD")
_REMOTE_DB = os.getenv(
    "BDM_DUCK_REMOTE",
    f"s3://{_BUCKET}/exploitation.duckdb",
)

_host = urllib.parse.urlparse(_MINIO_ENDPOINT).netloc or _MINIO_ENDPOINT
_use_ssl = "https" in _MINIO_ENDPOINT.lower()


def _ensure_httpfs(con: duckdb.DuckDBPyConnection) -> None:
    """Load httpfs, installing it on first use."""
    try:
        con.sql("LOAD httpfs;")
    except duckdb.IOException:
        # extension missing → download once, then load again
        con.sql("INSTALL httpfs;")
        con.sql("LOAD httpfs;")


# -----------------------------
#  Singleton connection
# --------------------------------
def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    _ensure_httpfs(con)
    con.execute(
        f"""
        CREATE OR REPLACE SECRET minio_secret (
          TYPE s3,
          PROVIDER config,
          KEY_ID    '{_USER}',
          SECRET    '{_PASS}',
          ENDPOINT  '{_host}',
          REGION    'us-east-1',
          URL_STYLE 'path',
          USE_SSL   {'true' if _use_ssl else 'false'},
          SCOPE     's3://{_BUCKET}/'
        );
        """
    )
    con.sql(f"ATTACH '{_REMOTE_DB}' AS expo_db (READ_ONLY);")
    return con


# Public handles
con: duckdb.DuckDBPyConnection = _connect()


def table(name: str) -> duckdb.DuckDBPyRelation:
    """Return a Relation for expo_db.<name> (lazy, chainable)."""
    return con.sql(f"SELECT * FROM expo_db.{name}")


def list_tables() -> list[str]:
    """Return all table names inside the exploitation snapshot."""
    q = """
    SELECT table_name
    FROM information_schema.tables
    """
    return [r[0] for r in con.sql(q).fetchall()]
