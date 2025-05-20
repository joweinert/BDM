from pyspark.sql.utils import AnalysisException
from util.delta_storage import DeltaStorageHandler
import datetime
from pyspark.sql import DataFrame
import json
import great_expectations as gx
from trusted_util import spark_ops as sp_o
import logging, os
from io import BytesIO
import imghdr
from PyPDF2 import PdfReader
import boto3
from botocore.client import Config

from pyspark.sql import functions as F


def init_logging():
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    if not os.path.exists("/opt/bitnami/spark/logs/trusted_run"):
        os.mkdir("/opt/bitnami/spark/logs/trusted_run")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f"/opt/bitnami/spark/logs/trusted_run/{ts}_trusted_run.log", mode="w"),
        ],
    )


def migrate(handler: DeltaStorageHandler, cfg: dict):
    """
    Find new ECB landing tables, convert each to tabular format, and append to the trusted indicators table.
    """
    # gx setup for this dataset
    context = gx.get_context(mode="file", project_root_dir="/opt/bitnami/spark/")
    validation_definition = context.validation_definitions.get(cfg["validation_definition_name"])
    # parsing config
    custom_fn = cfg.get("custom_fn")
    dedup_cols = cfg.get("dedup_cols")
    file_cfg = cfg.get("file_integrity", {})
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    targets = find_not_exported_landing_tables(handler)
    if not targets:
        return
    for table in sorted(targets):
        results = {"landing_table": table, "timestamp": ts, "success": True}
        try:
            logging.info(f"🔄 Processing table: {table}")
            df_landing = handler.read_table(table)
            results["incoming_schema"] = json.loads(df_landing.schema.json())

            df_tab = (custom_fn(df_landing, table) if custom_fn else df_landing).persist()

            existing = get_existing_trusted_table(handler, cfg["trusted_table"], cfg["trusted_storage_path"])
            df_new, results["cnt_orig"], results["cnt_dedup"] = sp_o.deduplicate(
                df_tab,
                keys=dedup_cols,
                df_to_comp=existing,
            )

            # gx validation
            results["gx_results"], results["quarantine"] = validate_table(
                handler, df_new, table, validation_definition
            )

            df_new, results["cnt_invalid_file"] = file_integrity_check(df_new, handler, file_cfg=file_cfg)

            results["outgoing_schema"] = json.loads(df_new.schema.json())

            if not results["quarantine"]:
                write_trusted_table(
                    handler,
                    df_new,
                    table,
                    datasource=cfg["trusted_datasource"],
                    dataset=cfg["trusted_dataset"],
                    storage_path=cfg["trusted_storage_path"],
                )

        finally:
            lineage_tracking(
                handler,
                results,
                cfg["trusted_storage_path"],
                cfg["trusted_datasource"],
                cfg["trusted_dataset"],
            )

    return get_existing_trusted_table(handler, cfg["trusted_table"], cfg["trusted_storage_path"])


def validate_table(handler, df, table, validation_definition):
    batch_parameters_dataframe = {"dataframe": df}
    results = validation_definition.run(batch_parameters=batch_parameters_dataframe).to_json_dict()
    if not results["success"]:
        handler.set_table_quarantine(table)
        logging.info(f"❌ Validation failed for {table}.")
        logging.info(f"Results: {results}")

    return results, not results["success"]


def lineage_tracking(handler, result: dict, zone: str = "Trusted_Zone", datasource: str = None, dataset: str = None):
    """
    Write a JSON audit record for this snapshot into MinIO under:
      deltalake/{zone}/{datasource}/{dataset}/_audit/{landing_table}_{timestamp}.json

    Args:
        handler (DeltaStorageHandler): delta storage handler instance
        result (dict): the result of the migration process
        zone (str): the zone name (folder) where we'll drop the audit file
        datasource (str): e.g. "imf"
        dataset (str): e.g. "indicators"
    """
    # where to write it
    # e.g. s3a://<bucket>/deltalake/trustedzone/imf/indicators/_audit/USA_NGDPD_06042025_162819_20250507_123456.json
    audit_path = (
        f"{zone}/{datasource}/{dataset}/_audit/"
        f"{result['landing_table'].replace('/','_')}_{result['timestamp']}.json"
    )
    handler.upload_json(path=audit_path, content=result)


def find_not_exported_landing_tables(handler):
    targets = handler.list_tables()
    if "quarantine" in targets:
        targets.remove("quarantine")
    storage_path = handler.get_storage_path()

    if not targets:
        logging.info(f"❌ No landing tables found for {storage_path}.")
        return

    not_exported = []
    for table in targets:
        # if handler.is_table_exported_or_quarantine(table):
        #     logging.info(f"⏭️  Table {table} already exported, skipping.")
        #     continue
        logging.info(f"✅ Table {table} is new and will be processed.")
        not_exported.append(table)

    return not_exported


def get_existing_trusted_table(handler, table_name, storage_path=None):
    if storage_path is not None:
        table_name = handler.get_delta_path(table_name, storage_path)
    try:
        existing = handler.read_table(table_name)
        logging.info(f"🔍 Loaded {existing.count()} existing records from trusted table.")
    except AnalysisException:
        existing = None
        logging.info("ℹ️ Trusted table not found; will create it on first write.")

    return existing


def write_trusted_table(
    handler: DeltaStorageHandler,
    cleaned_df,
    landing_table: str,
    datasource: str,
    dataset: str,
    storage_path: str = None,
):
    """Merge and write the DataFrame to the trusted zone.

    Args:
        handler (DeltaStorageHandler): The DeltaStorageHandler instance.
        cleaned_df (DataFrame): The DataFrame to write.
        datasource (str): The data source name.
        dataset (str): The dataset name.
        storage_path (str, optional): The storage path for the trusted table. Defaults to None -> uses handler storage path.
    """
    table = handler.write_df(
        df=cleaned_df, zone="Trusted", datasource=datasource, dataset=dataset, mode="append", storage_path=storage_path
    )
    logging.info(f"✅ Appended new records from {landing_table} into trusted table {table}.")
    handler.set_table_exported(landing_table)


def file_integrity_check(df, handler, file_cfg):
    """
    df               – your landing‐zone DataFrame
    handler          – DeltaStorageHandler (for .spark and .s3_client)
    file_cfg: dict with keys
       - enabled: bool
       - columns: List[str]   # e.g. ["doc_s3_path","face_s3_path"]
       - bytes_to_check: int
    Returns (valid_df, invalid_df, invalid_count)
    """
    if not file_cfg.get("enabled", False):
        return df, 0

    sc = handler.spark.sparkContext
    cols = file_cfg["columns"]
    n = file_cfg["bytes_to_check"]

    endpoint = os.environ["MINIO_ENDPOINT"]
    access_key = os.environ["MINIO_ROOT_USER"]
    secret_key = os.environ["MINIO_ROOT_PASSWORD"]
    region = "us-east-1"
    cfg = Config(signature_version="s3v4", s3={"addressing_style": "path"})

    # 1) For each column, build an RDD of its distinct URLs
    #    then filter in parallel for invalid URLs, and union them all
    def is_not_valid_url(url):
        # one boto3 client per executor
        global _s3c
        if "_s3c" not in globals():
            import boto3

            _s3c = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=cfg,
                region_name=region,
            )
        try:
            _, rest = url.split("://", 1)
            bucket, key = rest.split("/", 1)
            head = _s3c.head_object(Bucket=bucket, Key=key)
            size = head.get("ContentLength", 0)
            if size <= 0:
                return True
            to_read = min(n, size)
            first = _s3c.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{min(to_read, size)-1}")["Body"].read()
        except:
            return True
        # PDF?
        if first.startswith(b"%PDF-"):
            return False
        # Image?
        img_type = imghdr.what(None, first)
        if img_type is None:
            return True
        return False

    invalid_urls = set()

    for col in cols:
        url_rdd = df.select(col).distinct().rdd.map(lambda r: r[0])

        # collect invalid URLs for this column
        invalid_urls |= set(url_rdd.filter(is_not_valid_url).collect())

    # 2) Broadcast the full invalid‐URL set
    bc = sc.broadcast(invalid_urls)

    # 3) Build a boolean mask: row is invalid if ANY col’s value is in that set
    mask = None
    for col in cols:
        cond = F.col(col).isin(bc.value)
        mask = cond if mask is None else (mask | cond)

    # 4) Split into invalid_df / valid_df
    invalid_df = df.filter(mask)
    valid_df = df.filter(~mask)

    # 5) Count & return
    cnt_invalid = invalid_df.count()
    if cnt_invalid > 0:
        handler.write_df(
            df=invalid_df,
            path=f"s3a://{handler.minio_bucket}/{handler.get_storage_path()}/quarantine",  # from tzhere we loop the tables and merge them in trusted, so perfect
            mode="append",
        )
    logging.info(f"Sent {cnt_invalid} rows with defect files to Quarantine")
    return valid_df, cnt_invalid


# def file_integrity_check(df, handler, file_cfg: dict):
#     """
#     Parallel file-integrity check:
#       1) Turn distinct URLs into an RDD
#       2) On each executor, init a boto3 client once and validate URL
#       3) Collect valid URLs back to driver
#       4) Broadcast the set and filter the original DataFrame
#     """
#     if file_cfg.get("enabled") is False:
#         return df, None, 0

#     col_name = file_cfg["column"]
#     bytes_to_check = file_cfg["bytes_to_check"]

#     spark = handler.spark
#     sc = spark.sparkContext

#     # 1) Distinct URL RDD
#     url_rdd = df.select(col_name).distinct().rdd.map(lambda row: row[0])

#     # 2) Executor-side validator
#     def is_not_valid_url(url):
#         # lazy-init one S3 client per executor
#         global _s3_client
#         if "_s3_client" not in globals():
#             _s3_client = boto3.client(
#                 "s3",
#                 endpoint_url=handler.s3_client._endpoint.host,  # inherits your MinIO endpoint
#                 aws_access_key_id=handler.s3_client._request_signer._credentials.access_key,
#                 aws_secret_access_key=handler.s3_client._request_signer._credentials.secret_key,
#                 config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
#                 region_name="spain",
#             )

#         client = _s3_client

#         # parse "s3a://bucket/key/with/slashes"
#         try:
#             _, rest = url.split("://", 1)
#             bucket, key = rest.split("/", 1)
#         except Exception:
#             return False

#         # 2A) HEAD + size check
#         try:
#             head = client.head_object(Bucket=bucket, Key=key)
#             if head.get("ContentLength", 0) <= 0:
#                 return True
#             resp = client.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{bytes_to_check-1}")
#             data = resp["Body"].read()
#         except Exception:
#             return True

#         # 2B) PDF magic
#         if data.startswith(b"%PDF-"):
#             try:
#                 PdfReader(BytesIO(data + b"\n%%EOF"))
#                 return False
#             except Exception:
#                 return True

#         # 2C) Image magic
#         return imghdr.what(None, data) is None

#     # 3) Run the check in parallel
#     invalid_urls = url_rdd.filter(is_not_valid_url).collect()

#     # 4) Broadcast and filter
#     bc = sc.broadcast(set(invalid_urls))
#     invalid_df = df.filter(F.col(col_name).isin(bc.value))
#     valid_df = df.filter(~F.col(col_name).isin(bc.value))

#     invalid_cnt = invalid_df.count()

#     print(f"Sent {invalid_cnt} rows with defect files to Quarantine")


#     return valid_df, invalid_df, invalid_cnt
