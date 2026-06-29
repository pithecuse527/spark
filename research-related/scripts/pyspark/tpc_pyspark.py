"""Load TPC-DS or TPC-H Parquet tables as Spark SQL temporary views.

Import this module from a PySpark shell or notebook, then call
``register_tables(spark, "tpcds")``.  The table names become directly
queryable with ``spark.sql(...)``.
"""

from __future__ import annotations

from typing import Final, Iterable, Union


DEFAULT_DATA_BASE: Final = "s3a://spark-obj-storage"

TPCDS_TABLES: Final = (
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
)

TPCH_TABLES: Final = (
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
)


def data_location(
    benchmark: str, scale: Union[int, str], data_base: str = DEFAULT_DATA_BASE
) -> str:
    """Return the existing benchmark data root for a benchmark and scale."""
    benchmark = benchmark.lower()
    if benchmark not in ("tpcds", "tpch"):
        raise ValueError("benchmark must be 'tpcds' or 'tpch'")
    return f"{data_base.rstrip('/')}/{benchmark}-scale-{scale}"


def table_names(benchmark: str) -> Iterable[str]:
    """Return the Parquet table directory names for *benchmark*."""
    benchmark = benchmark.lower()
    if benchmark == "tpcds":
        return TPCDS_TABLES
    if benchmark == "tpch":
        return TPCH_TABLES
    raise ValueError("benchmark must be 'tpcds' or 'tpch'")


def register_tables(
    spark,
    benchmark: str = "tpcds",
    scale: Union[int, str] = 100,
    data_base: str = DEFAULT_DATA_BASE,
) -> str:
    """Register each Parquet table as a session-scoped temporary SQL view.

    Returns the benchmark data root.  Existing views with the same table name
    are replaced, which makes it safe to call again after changing datasets.
    """
    root = data_location(benchmark, scale, data_base)
    for table in table_names(benchmark):
        spark.read.parquet(f"{root}/{table}").createOrReplaceTempView(table)
    return root
