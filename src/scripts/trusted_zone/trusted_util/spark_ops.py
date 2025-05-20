from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from typing import List, Optional
import logging


def deduplicate(
    df: DataFrame,
    keys: List[str],
    df_to_comp: Optional[DataFrame] = None,
    keep: str = "first",
    order_by: Optional[List[str]] = None,
) -> DataFrame:
    """
    Remove duplicates from `df` by `keys`, optionally only keeping the first or last
    row per key, then (if df_to_comp is provided) remove any keys already present
    in df_to_comp via an anti-join.

    Args:
      df: Input DataFrame.
      keys: columns to use as the unique key.
      df_to_comp: If provided, a DataFrame of existing rows to exclude.
      keep: 'first' or 'last' (only meaningful if you also pass `order_by`).
      order_by: columns to define ordering for 'first'/'last' selection.

    Returns:
      A DataFrame with duplicates removed and optionally already-existing rows excluded.
    """
    cnt_before = df.count()
    if order_by:
        w = Window.partitionBy(*keys).orderBy(
            *([F.col(c).asc() for c in order_by] if keep == "first" else [F.col(c).desc() for c in order_by])
        )
        df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    else:
        # simple dropDuplicates (keeps arbitrary row when keep='first' or 'last')
        df = df.dropDuplicates(subset=keys)

    # if theres a comparison df, anti-join it out -> existing rows
    if df_to_comp is not None:
        df = df.join(df_to_comp.select(*keys).distinct(), on=keys, how="anti")

    cnt_after = df.count()
    logging.info(f"ℹ️ Deduplicated {cnt_before} rows to {cnt_after} rows")

    return df, cnt_before, cnt_after


def explode_map(df, map_expr, key_col, val_col):
    """
    Explode a MapType column into two separate columns (key and value).

    This helper takes an existing DataFrame and a Column expression referencing a MapType,
    then:
      1. Uses `map_entries` to convert the map into an array of (key, value) structs.
      2. `explode`s that array into individual rows, each containing one key value pair.
      3. Selects all original columns (except the temporary helper column), plus two new columns
         named according to `key_col` and `val_col`, holding the maps key and value respectively.

    Args:
        df (DataFrame): Input Spark DataFrame.
        map_expr (Column): A Column referencing the MapType column to explode.
        key_col (str): Name for the output column containing map keys.
        val_col (str): Name for the output column containing map values.

    Returns:
        DataFrame: A new DataFrame with one row per map entry and two additional columns:
                   `key_col` (the map key) and `val_col` (the map value).
    """
    return df.withColumn("_kv", F.explode(F.map_entries(map_expr))).select(
        *[c for c in df.columns if c != "_kv"], F.col("_kv.key").alias(key_col), F.col("_kv.value").alias(val_col)
    )


def build_map_from_cols(col_names, cast_type, return_map=False):
    """
    Construct a Spark MapType Column from a list of column names by casting each column's values.

    This helper builds a map of key→value entries for the given columns:
        {'col1': cast(`col1` AS cast_type), 'col2': cast(`col2` AS cast_type), …}
    and returns a single Column of MapType, so you can directly explode it or manipulate it further.

    Args:
        col_names (List[str]): Names of the DataFrame columns to include in the map.
        cast_type (DataType): The Spark DataType to cast each column's value to.
        return_map (bool): If True, return a single MapType Column instead of a list of expressions.

    Returns:
        Column: A `create_map` Column mapping each column name to its casted value.
    """
    exprs = []
    for name in col_names:
        exprs.append(F.lit(name))
        exprs.append(F.col(f"`{name}`").cast(cast_type))
    
    if return_map:
        return F.create_map(*exprs)
    return exprs

def build_map_from_struct(df, struct_path: str, return_map: bool = False):
    """
    Construct a list of literal-column pairs for creating a Spark map from the fields of a nested StructType column.

    This helper inspects the schema of the given DataFrame, navigates through nested StructType fields
    according to the dot-separated path, and returns a flat list:
        [lit(key1), col("struct_path.`key1`"), lit(key2), col("struct_path.`key2`"), …]
    which can be passed directly into pyspark.sql.functions.create_map(*elements).

    Args:
        df (DataFrame): The Spark DataFrame containing the target StructType column.
        struct_path (str): Dot-separated path to the StructType column (e.g. "parent.child.substruct").
        return_map (bool): If True, return a single MapType Column instead of a list of expressions.

    Returns:
        List[Column]: A list of alternating lit(key) and col("struct_path.`key`") expressions.

    Raises:
        ValueError: If any part of struct_path does not exist in the schema or does not resolve to a StructType.
    """
    parts = struct_path.split(".")
    # getting the schema
    fields = df.schema.fields
    curr_type = None

    # traversing into nested StructTypes according to parts
    for part in parts:
        field = next((f for f in fields if f.name == part), None)
        if field is None:
            raise ValueError(f"Column path '{struct_path}' not found in schema")
        curr_type = field.dataType
        if isinstance(curr_type, StructType):
            fields = curr_type.fields
        else:
            # if this isnt a StructType, we cant explode its fields
            raise ValueError(f"Field '{part}' in path '{struct_path}' is not a StructType")

    # now curr_type is the StructType at struct_path
    keys = [f.name for f in curr_type.fields]
    elements = []
    for k in keys:
        # we need backticks around k in case of colons or dots
        elements.append(F.lit(k))
        elements.append(F.col(f"{struct_path}.`{k}`"))

    # Return either the raw expressions or the MapType Column
    if return_map:
        return F.create_map(*elements)
    return elements


# def posexplode_values(df, array_col, idx_name="idx", val_name="val"):
#     """
#     posexplode an ArrayType column, yielding (idx,val).
#     """
#     return df.select(F.posexplode(array_col).alias(idx_name, val_name)).withColumn(
#         val_name, F.col(f"{val_name}.id")
#     )  # if each val is a struct with id


# def explode_array(df, col_name, new_col):
#     return df.withColumn(new_col, F.explode_outer(col_name))


# def explode_dict_column(df, column_path: str, key_col: str = "key", value_col: str = "value"):
#     """
#     Explodes a nested dict (MapType) column into (key, value) rows.

#     Args:
#         df: input DataFrame
#         column_path: dot path to the column with dict data (e.g. 'x.y.observations')
#         key_col: name for resulting key column
#         value_col: name for resulting value column

#     Returns:
#         DataFrame with (key_col, value_col)
#     """
#     return df.selectExpr(f"explode({column_path}) as ({key_col}, {value_col})")


# def map_index_to_column(df, index_col, mapping_dict, new_col):
#     """
#     Maps an index column to a new column based on a dictionary.
#     """

#     @udf(StringType())
#     def _map_index(index):
#         return mapping_dict.get(index)

#     return df.withColumn(new_col, _map_index(col(index_col)))


# from pyspark.sql import DataFrame
# from pyspark.sql.functions import (
#     col,
#     lower,
#     regexp_replace,
#     to_timestamp,
#     when,
#     trim,
#     length,
# )
# from pyspark.sql.types import StructType


# def unwrap_common_wrappers(record: dict, known_wrappers=("data", "result", "response")):
#     for key in known_wrappers:
#         if key in record and isinstance(record[key], dict):
#             return unwrap_common_wrappers(record[key], known_wrappers)
#     return record

# from collections.abc import MutableMapping


# def flatten_dict(d, parent_key="", sep="."):
#     items = []
#     for k, v in d.items():
#         new_key = f"{parent_key}{sep}{k}" if parent_key else k
#         if isinstance(v, MutableMapping):
#             items.extend(flatten_dict(v, new_key, sep=sep).items())
#         else:
#             items.append((new_key, v))
#     return dict(items)


# from dateutil.parser import parse as parse_date


# def normalize_timestamps(record):
#     for key in record:
#         if "time" in key.lower() or "date" in key.lower():
#             try:
#                 record[key] = parse_date(str(record[key])).isoformat()
#             except Exception:
#                 pass
#     return record


# def clean_json_record(record):
#     if not isinstance(record, dict):
#         return None  # Skip non-dicts

#     record = unwrap_common_wrappers(record)
#     record = normalize_timestamps(record)
#     # Optional flattening
#     # record = flatten_dict(record)

#     # Example cleanup: remove nulls, filter types
#     return {k: v for k, v in record.items() if v not in (None, "", [], {})}


# def enforce_schema(df: DataFrame, expected_schema: dict) -> DataFrame:
#     for col_name, dtype in expected_schema.items():
#         df = df.withColumn(col_name, col(col_name).cast(dtype))
#     return df


# def drop_duplicates(df: DataFrame, subset: list) -> DataFrame:
#     return df.dropDuplicates(subset=subset)


# def filter_nulls(df: DataFrame, required_columns: list) -> DataFrame:
#     for col_name in required_columns:
#         df = df.filter(col(col_name).isNotNull())
#     return df


# def validate_age(df: DataFrame, col_name: str = "age") -> DataFrame:
#     return df.filter((col(col_name) >= 0) & (col(col_name) < 120))


# def validate_timestamp(df: DataFrame, col_name: str) -> DataFrame:
#     return df.withColumn(col_name, to_timestamp(col(col_name))).filter(col(col_name).isNotNull())


# def normalize_text(df: DataFrame, columns: list) -> DataFrame:
#     for col_name in columns:
#         df = df.withColumn(col_name, lower(regexp_replace(col(col_name), r"[^\w\s]", "")))
#     return df


# def trim_whitespace(df: DataFrame, columns: list) -> DataFrame:
#     for col_name in columns:
#         df = df.withColumn(col_name, trim(col(col_name)))
#     return df


# def filter_short_text(df: DataFrame, column: str, min_len: int = 3) -> DataFrame:
#     return df.filter(length(col(column)) >= min_len)


# def replace_null_with_default(df: DataFrame, replacements: dict) -> DataFrame:
#     for col_name, default_value in replacements.items():
#         df = df.withColumn(col_name, when(col(col_name).isNull(), default_value).otherwise(col(col_name)))
#     return df


# def validate_positive_numeric(df: DataFrame, column: str) -> DataFrame:
#     return df.filter(col(column) >= 0)


# def flatten_structs(df: DataFrame) -> DataFrame:
#     for field in df.schema.fields:
#         if hasattr(field.dataType, "fields"):
#             for subfield in field.dataType.names:
#                 df = df.withColumn(f"{field.name}_{subfield}", col(f"{field.name}.{subfield}"))
#             df = df.drop(field.name)
#     return df
