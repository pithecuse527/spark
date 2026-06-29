"""PySpark shell startup hook that exposes TPC tables as SQL temporary views."""

import os

from tpc_pyspark import DEFAULT_DATA_BASE, register_tables


BENCHMARK = os.environ.get("TPC_BENCHMARK", "tpcds")
SCALE = os.environ.get("TPC_SCALE", "100")
DATA_BASE = os.environ.get("TPC_DATA_BASE", DEFAULT_DATA_BASE)

data_root = register_tables(spark, BENCHMARK, SCALE, DATA_BASE)
print(f"Registered {BENCHMARK.upper()} SF{SCALE} tables from {data_root}")
print('Run SQL with: spark.sql("SELECT * FROM store_sales LIMIT 10").show()')
