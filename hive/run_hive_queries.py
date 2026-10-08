"""
hive/run_hive_queries.py
------------------------
PPT layer 3 - APACHE HIVE (querying the data lake with HiveQL).

1. Starts Spark WITH Hive support (Hive metastore stored in a small Derby
   database in the Docker volume /hive).
2. Runs hive/create_tables.hql -> registers the Bronze/Silver/Gold Parquet
   folders as external Hive tables in database "wearable_health".
3. Runs every query in hive/queries.hql and prints the results.

Why not a separate Hive server? HiveServer2 + a metastore database need
several extra GB of RAM. Spark SQL has Hive built in (same metastore, same
HiveQL), so on a laptop we run HiveQL through Spark.

Run:  docker compose run --rm hive-queries
"""

import os
import re
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import settings as S  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))


def build_spark_with_hive():
    """Spark session connected to the Hive metastore."""
    os.makedirs(S.HIVE_DIR, exist_ok=True)
    spark = (
        SparkSession.builder
        .appName("WearableHealth-HiveQueries")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        # --- Hive metastore settings ---
        .config("spark.sql.warehouse.dir", os.path.join(S.HIVE_DIR, "warehouse"))
        .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={os.path.join(S.HIVE_DIR, 'metastore_db')};create=true")
        .config("spark.sql.hive.manageFilesourcePartitions", "false")   # read date partitions straight from the folders
        .config("spark.sql.files.ignoreCorruptFiles", "true")          # skip a file that is half-written
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def split_statements(path):
    """Split a .hql file into (title, sql) pairs. The title is the '-- Qn:' line, if any."""
    with open(path) as f:
        text = f.read().replace("${DATA_DIR}", S.DATA_DIR)
    statements, buffer, title = [], [], None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):                      # comment line
            match = re.match(r"^--\s*(Q\d+:.*)$", stripped)
            if match:
                title = match.group(1).strip()             # remember the query title
            continue
        buffer.append(line)
        if stripped.endswith(";"):                         # end of one statement
            sql = "\n".join(buffer).strip().rstrip(";").strip()
            if sql:
                statements.append((title, sql))
            buffer, title = [], None
    leftover = "\n".join(buffer).strip()
    if leftover:
        statements.append((title, leftover))
    return statements


def main():
    spark = build_spark_with_hive()

    # ---- 1. create / refresh the external tables ----
    print("\n==================== CREATING HIVE TABLES ====================")
    for _title, sql in split_statements(os.path.join(HERE, "create_tables.hql")):
        first_line = sql.splitlines()[0]
        try:
            spark.sql(sql)
            if first_line.upper().startswith("CREATE TABLE"):
                print(f"  OK   {first_line}")
        except Exception as exc:
            reason = str(exc).splitlines()[0][:110]
            print(f"  SKIP {first_line}\n       -> {reason}\n       (the folder has no data yet - run the earlier phases first)")

    spark.sql(f"USE {S.HIVE_DATABASE}")
    print("\nTables in database wearable_health:")
    spark.sql("SHOW TABLES").show(truncate=False)

    # ---- 2. run the example queries ----
    print("==================== EXAMPLE HIVEQL QUERIES ====================")
    for title, sql in split_statements(os.path.join(HERE, "queries.hql")):
        print(f"\n### {title or 'query'}")
        try:
            spark.sql(sql).show(25, truncate=False)
        except Exception as exc:
            reason = str(exc).splitlines()[0][:110]
            print(f"  (skipped: {reason})")

    spark.stop()


if __name__ == "__main__":
    main()
