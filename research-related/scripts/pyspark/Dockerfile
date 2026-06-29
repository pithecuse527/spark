# In-cluster client-mode Spark driver + JupyterLab, with the screening code baked in.
# Replaces the runtime `pip install` + `kubectl cp` approach.
#
# Build (Mac arm64 -> cluster amd64) and push:
#   docker buildx build --platform linux/amd64 \
#     -t gihong96/spark-screening:v1 --push .
#
# Base: Apache Spark 4.1.2, Scala 2.13, Java 21, Python 3.10 (python3 variant —
# the plain -ubuntu tag ships no Python).
FROM apache/spark:4.1.2-scala2.13-java21-python3-ubuntu

USER root

# JupyterLab (Python 3.10 -> no version pin needed)
RUN pip3 install --no-cache-dir jupyterlab jupyter-server-proxy

# S3A for the s3a:// event log (vanilla apache/spark omits these). Versions must
# match the bundled Hadoop (3.4.2) and its declared AWS SDK v2 bundle (2.29.52).
RUN cd /opt/spark/jars && \
    curl -fsSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar && \
    curl -fsSLO https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar

ENV SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip:/work

WORKDIR /work

# Screening logic + notebook (no kubectl cp needed).
COPY screening.py tpc_pyspark.py run_screening.ipynb run_nb_cells.py log4j2-aqe.properties /work/

# TPC-DS / TPC-H query .sql files (from sql/core/src/test/resources via deploy.sh).
# Referable in JupyterLab and loadable with screening.load_sql("tpcds/q9").
COPY queries /work/queries

EXPOSE 8888 7078 7079

CMD ["bash", "-lc", "jupyter lab \
  --ip=0.0.0.0 --port=8888 --no-browser --allow-root \
  --ServerApp.base_url=/jupyter \
  --ServerApp.token='' --ServerApp.password='' \
  --ServerApp.allow_origin='*' --ServerApp.allow_remote_access=True \
  --notebook-dir=/work"]
