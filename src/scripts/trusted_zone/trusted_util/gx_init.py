import great_expectations as gx
from great_expectations import expectations as gxe
import os
import shutil
import datetime

# ---------------------------------------------------------------------------
# Configuration dictionaries – one per datasource (extend as needed)
# ---------------------------------------------------------------------------

OPS_DB_CONFIG = {
    "kyc_metadata": {
        "expectation_suite_name": "ops_db_kyc_suite",
        "data_source_name": "ops_db",
        "data_asset_name": "kyc_metadata",
        "batch_definition_name": "ops_db_batch_definition",
        "validation_definition_name": "ops_db_kyc_validation_definition",
    },
    "pdf_metadata": {
        "expectation_suite_name": "ops_db_pdf_suite",
        "data_source_name": "ops_db",
        "data_asset_name": "pdf_metadata",
        "batch_definition_name": "ops_db_batch_definition",
        "validation_definition_name": "ops_db_pdf_validation_definition",
    },
    "user_profiles": {
        "expectation_suite_name": "ops_db_user_suite",
        "data_source_name": "ops_db",
        "data_asset_name": "user_profiles",
        "batch_definition_name": "ops_db_batch_definition",
        "validation_definition_name": "ops_db_user_validation_definition",
    },
    "user_transactions": {
        "expectation_suite_name": "ops_db_transaction_suite",
        "data_source_name": "ops_db",
        "data_asset_name": "user_transactions",
        "batch_definition_name": "ops_db_batch_definition",
        "validation_definition_name": "ops_db_transaction_validation_definition",
    },
}


IMF_CONFIG = {
    "indicators": {
        "expectation_suite_name": "imf_suite",
        "data_source_name": "imf",
        "data_asset_name": "indicators",
        "batch_definition_name": "imf_batch_definition",
        "validation_definition_name": "imf_validation_definition",
    },
}

ECB_CONFIG = {
    "exchange_rates": {
        "expectation_suite_name": "ecb_suite",
        "data_source_name": "ecb",
        "data_asset_name": "exchange_rates",
        "batch_definition_name": "ecb_batch_definition",
        "validation_definition_name": "ecb_validation_definition",
    },
}

EODHD_CONFIG = {
    "financial_news": {
        "expectation_suite_name": "eodhd_suite",
        "data_source_name": "eodhd",
        "data_asset_name": "financial_news",
        "batch_definition_name": "eodhd_batch_definition",
        "validation_definition_name": "eodhd_validation_definition",
    },
}

FINNHUB_CONFIG = {
    "financial_report": {
        "expectation_suite_name": "finnhub_suite",
        "data_source_name": "finnhub",
        "data_asset_name": "financial_report",
        "batch_definition_name": "finnhub_batch_definition",
        "validation_definition_name": "finnhub_validation_definition",
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_gx_stores():
    """Ensure GE stores live side by side with project code."""
    context = gx.get_context(mode="file", project_root_dir="/opt/bitnami/spark/")

    paths = {
        "expectations_store": "expectations",
        "validation_definition_store": "validation_definitions",
        "checkpoint_store": "checkpoints",
        "validation_results_store": "validation_results",
    }

    for store_name, rel_path in paths.items():
        context.variables.config.stores[store_name]["store_backend"]["base_directory"] = rel_path

    context.variables.save()
    return context


def _setup_validator(context, cfg):
    """Create datasource -> asset -> batch -> suite -> validation definition chain."""
    suite = gx.ExpectationSuite(name=cfg["expectation_suite_name"])
    suite = context.suites.add(suite)

    ds_name = cfg["data_source_name"]
    all_sources = context.data_sources.all()
    if ds_name not in all_sources:
        context.data_sources.add_spark(ds_name)
    data_source = context.data_sources.get(ds_name)
    data_asset = data_source.add_dataframe_asset(name=cfg["data_asset_name"])
    data_asset.add_batch_definition_whole_dataframe(cfg["batch_definition_name"])

    validation_definition = gx.ValidationDefinition(
        data=data_asset.get_batch_definition(cfg["batch_definition_name"]),
        suite=suite,
        name=cfg["validation_definition_name"],
    )
    context.validation_definitions.add(validation_definition)

    return suite



def setup_imf_validators(context):
    cfg = IMF_CONFIG["indicators"]
    suite = _setup_validator(context, cfg)

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="year"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="year", min_value=1900, max_value=2100))
    suite.add_expectation(gxe.ExpectColumnValuesToBeIncreasing(column="year", strictly=True))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="indicator"))
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="indicator", regex=r"^[A-Z_]+$"))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="value"))
    # suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="value", min_value=-1.0, strict_min=True)) #there is
    suite.add_expectation(gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(column_list=["year", "indicator"]))


def setup_ecb_validators(context):
    cfg = ECB_CONFIG["exchange_rates"]
    suite = _setup_validator(context, cfg)

    # time
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="time"))
    # Ensure date values fall within reasonable historical FX date window
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(
            column="time",
            min_value="1999-01-04",  # first EUR trading day
            max_value=datetime.date.today(),
        )
    )

    # currency
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="currency"))
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="currency", regex=r"^[A-Z]{3}$"))

    # rate
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="rate"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="rate", min_value=0.0, max_value=1e6))

    # uniqueness: one row per time per currency
    suite.add_expectation(gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(column_list=["time", "currency"]))


def setup_eodhd_validators(context):
    """News articles from EODHD -> Trusted_Zone.eodhd.financial_news"""
    cfg = EODHD_CONFIG["financial_news"]
    suite = _setup_validator(context, cfg)

    # required + format
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="ts"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="title"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="link"))
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="link", regex=r"^https?://"))

    # sentiment scores
    for col in ("sent_neg", "sent_neu", "sent_pos"):
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column=col, min_value=0, max_value=1))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="sent_polarity", min_value=-1, max_value=1))

    # business key – one row per article
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="link"))


def setup_finnhub_validators(context):
    """Quarterly SEC fundamentals from Finnhub → Trusted_Zone.finnhub.financial_report"""
    cfg = FINNHUB_CONFIG["financial_report"]
    suite = _setup_validator(context, cfg)

    # identifiers & metadata
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="symbol"))
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="symbol", regex=r"^[A-Z\.]{1,10}$"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="year"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="year", min_value=1900, max_value=2100))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="quarter"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="quarter", min_value=1, max_value=4))

    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="statement", value_set=["bs", "cf", "ic"]))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="concept"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="value"))

    # compound key – each metric appears once per filing
    suite.add_expectation(
        gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(
            column_list=["symbol", "year", "quarter", "statement", "concept"]
        )
    )


def setup_ops_db_validators(context):
    """ops db expectations"""
    # ── user_transactions ────────────────────────────────────────────────────────
    cfg = OPS_DB_CONFIG["user_transactions"]
    suite = _setup_validator(context, cfg)

    # transaction_id is a UUID
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="transaction_id"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToMatchRegex(
            column="transaction_id",
            # lowercase hex + hyphens
            regex=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        )
    )

    # each transaction must have a timestamp
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="timestamp"))

    # type should be Debit or Credit
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="type", value_set=["Debit", "Credit"]))

    # amount is ≥0 and ≤1e6
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="amount"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="amount", min_value=0, max_value=1e6))

    # currency is a 3-letter uppercase code
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="currency"))
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="currency", regex=r"^[A-Z]{3}$"))

    # description like “Mnnn – …”
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="description", regex=r"^M\d{3} - .+"))

    # location and device_id must be present
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="location"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="device_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="device_id", regex=r"^D\d{6}$"))

    # valid IPv4 address
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="ip_address", regex=r"^(?:\d{1,3}\.){3}\d{1,3}$"))

    # account_balance ≥ amount and non-null
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="account_balance"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="account_balance", min_value=0, max_value=1e8))

    # previous_transaction timestamp string
    suite.add_expectation(
        gxe.ExpectColumnValuesToMatchRegex(
            column="previous_transaction", regex=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
        )
    )

    # transaction_duration and login_attempts reasonable
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(column="transaction_duration", min_value=0, max_value=86400)
    )
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="login_attempts", min_value=0, max_value=10))

    # unique transaction_id
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="transaction_id"))
    # ── end user_transactions ────────────────────────────────────────────────────

    # ── user_profiles ────────────────────────────────────────────────────────────
    cfg = OPS_DB_CONFIG["user_profiles"]
    suite = _setup_validator(context, cfg)

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="user_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="user_id", regex=r"^AC\d{5}$"))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="age"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="age", min_value=0, max_value=120))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="occupation"))

    # unique user_id
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="user_id"))
    # ── end user_profiles ────────────────────────────────────────────────────────

    # ── pdf_metadata ─────────────────────────────────────────────────────────────
    cfg = OPS_DB_CONFIG["pdf_metadata"]
    suite = _setup_validator(context, cfg)

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="request_id"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToMatchRegex(
            column="request_id", regex=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        )
    )

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="user_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="user_id", regex=r"^AC\d{5}$"))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="category"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column="category", value_set=["contract", "receipt", "report"])
    )

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="pdf_s3_path"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToMatchRegex(column="pdf_s3_path", regex=r"^s3a://unstructureddata/pdfs/.+\.pdf$")
    )

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="upload_time"))

    # unique request_id
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="request_id"))
    # ── end pdf_metadata ─────────────────────────────────────────────────────────

    # ── kyc_metadata ─────────────────────────────────────────────────────────────
    cfg = OPS_DB_CONFIG["kyc_metadata"]
    suite = _setup_validator(context, cfg)

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="request_id"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToMatchRegex(
            column="request_id", regex=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        )
    )

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="user_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column="user_id", regex=r"^AC\d{5}$"))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="doc_s3_path"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToMatchRegex(column="doc_s3_path", regex=r"^s3a://unstructureddata/images/doc_.+\.jpg$")
    )

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="face_s3_path"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToMatchRegex(
            column="face_s3_path", regex=r"^s3a://unstructureddata/images/face_.+\.jpg$"
        )
    )

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="upload_time"))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="status"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column="status", value_set=["pending", "approved", "rejected"])
    )

    # unique request_id
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="request_id"))


def init_gx_context(override: bool = False):
    gx_root = "/opt/bitnami/spark/gx"
    already_exists = os.path.exists(os.path.join(gx_root, "great_expectations.yml"))

    if already_exists and not override:
        print("Great Expectations context already initialized.")
        return

    if override and already_exists:
        shutil.rmtree(gx_root, ignore_errors=True)
        print("Great Expectations context removed. Reinitializing…")

    context = _setup_gx_stores()

    # Register validators per datasource
    setup_ops_db_validators(context)
    setup_imf_validators(context)
    setup_ecb_validators(context)
    setup_eodhd_validators(context)
    setup_finnhub_validators(context)

    print("✅ Great Expectations context initialized and all expectation suites registered.")


if __name__ == "__main__":
    init_gx_context(override=True)
