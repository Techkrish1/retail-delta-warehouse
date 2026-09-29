# Databricks notebook source
# MAGIC %md
# MAGIC # Retail Data Warehouse — Customer Dimension (SCD Type 2)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Context
# MAGIC
# MAGIC In a retail company, customer data is one of the most critical dimensions in the warehouse.
# MAGIC Customers move, change emails, update phone numbers — and **business users need to answer historical questions**:
# MAGIC
# MAGIC - *"What was the customer's address when they placed that order in 2023?"*
# MAGIC - *"How many customers lived in California last year vs. this year?"*
# MAGIC - *"Which customers changed regions after our marketing campaign?"*
# MAGIC
# MAGIC A simple `UPDATE` would overwrite the old data and **destroy the ability to answer these questions**.
# MAGIC This is exactly the problem **Slowly Changing Dimension Type 2 (SCD2)** solves.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What is SCD Type 2?
# MAGIC
# MAGIC SCD2 is a data warehousing pattern where **every change to a dimension record creates a new row**
# MAGIC rather than overwriting the existing one. The old row is "expired" (closed), and the new row becomes active.
# MAGIC
# MAGIC | Type | Behavior | Use When |
# MAGIC |------|----------|----------|
# MAGIC | SCD1 | Overwrite — no history kept | History doesn't matter (e.g., typo fix) |
# MAGIC | **SCD2** | **New row per change — full history** | **Need to track what changed and when** |
# MAGIC | SCD3 | Add a column for "previous value" | Only last change matters |
# MAGIC
# MAGIC **We implement SCD2** because the business needs complete history for reporting and compliance.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC [Source System / CRM]
# MAGIC         │
# MAGIC         ▼
# MAGIC  [Staging Table]          ← Raw incoming changes (daily batch / CDC feed)
# MAGIC         │
# MAGIC         ▼
# MAGIC  [SCD2 MERGE Logic]       ← Delta Lake MERGE handles expire + insert atomically
# MAGIC         │
# MAGIC         ▼
# MAGIC  [customer_dim]           ← Delta table with full version history
# MAGIC         │
# MAGIC         ▼
# MAGIC  [Gold / Reporting Layer] ← BI tools query is_current = true for latest state
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Table Design Decisions
# MAGIC
# MAGIC | Column | Type | Why it exists |
# MAGIC |--------|------|--------------|
# MAGIC | `surrogate_key` | BIGINT | Primary key — natural key repeats across versions, so we need a unique row identifier |
# MAGIC | `customer_id` | INT | Natural/business key from the source system — used for matching |
# MAGIC | `customer_name` | STRING | Tracked attribute |
# MAGIC | `email` | STRING | Tracked attribute — changes trigger a new version |
# MAGIC | `address` | STRING | Tracked attribute — changes trigger a new version |
# MAGIC | `city` | STRING | Tracked attribute |
# MAGIC | `country` | STRING | Tracked attribute |
# MAGIC | `eff_start_date` | DATE | When this version became valid |
# MAGIC | `eff_end_date` | DATE | When this version was superseded — `9999-12-31` means "currently active" |
# MAGIC | `is_current` | BOOLEAN | Fast filter for active records — avoids date arithmetic in every query |
# MAGIC | `created_at` | TIMESTAMP | Audit — when was this row written to the warehouse |
# MAGIC | `updated_at` | TIMESTAMP | Audit — when was this row last modified |
# MAGIC
# MAGIC > **Why `9999-12-31` instead of NULL for open-ended records?**
# MAGIC > Using NULL for active records makes range queries awkward — you'd need `WHERE eff_end_date IS NULL OR eff_end_date >= query_date`.
# MAGIC > A sentinel date like `9999-12-31` means every active record has a value, and the query becomes a clean `BETWEEN` or simple inequality.
# MAGIC
# MAGIC > **Why `is_current` if we already have `eff_end_date`?**
# MAGIC > `is_current` is a denormalized convenience column. Querying `WHERE is_current = true` is faster and more readable than
# MAGIC > `WHERE eff_end_date = '9999-12-31'`. Both are equivalent, but `is_current` makes intent explicit.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup — Imports and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, current_date, lit,
    to_date, when, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    BooleanType, DateType, TimestampType, LongType
)
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# Ensure Delta Lake optimizations are enabled
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Configuration — centralizing these makes the notebook easier to maintain
DATABASE_NAME   = "retail"
TABLE_NAME      = "customer_dim"
FULL_TABLE_NAME = f"{DATABASE_NAME}.{TABLE_NAME}"
OPEN_END_DATE   = "9999-12-31"    # sentinel for currently active records

print(f"Target table : {FULL_TABLE_NAME}")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Create the Database and Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS retail
# MAGIC COMMENT 'Retail data warehouse — dimension and fact tables';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop for a clean demo run. In production you would NEVER drop a dimension table.
# MAGIC DROP TABLE IF EXISTS retail.customer_dim;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail.customer_dim (
# MAGIC     surrogate_key   BIGINT      COMMENT 'Unique row identifier — auto-incremented per version',
# MAGIC     customer_id     INT         COMMENT 'Business / natural key from source CRM',
# MAGIC     customer_name   STRING      COMMENT 'Full name',
# MAGIC     email           STRING      COMMENT 'Contact email',
# MAGIC     address         STRING      COMMENT 'Street address',
# MAGIC     city            STRING      COMMENT 'City',
# MAGIC     country         STRING      COMMENT 'Country',
# MAGIC     eff_start_date  DATE        COMMENT 'Date from which this version is valid',
# MAGIC     eff_end_date    DATE        COMMENT 'Date until which this version is valid; 9999-12-31 = currently active',
# MAGIC     is_current      BOOLEAN     COMMENT 'True for the active/latest version of this customer',
# MAGIC     created_at      TIMESTAMP   COMMENT 'Row insertion timestamp',
# MAGIC     updated_at      TIMESTAMP   COMMENT 'Row last-update timestamp'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Customer dimension table — SCD Type 2, full change history retained'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact'   = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC > **Why these TBLPROPERTIES?**
# MAGIC >
# MAGIC > - `optimizeWrite`: Databricks automatically coalesces small files during writes, preventing the "small files problem" that degrades query performance over time.
# MAGIC > - `autoCompact`: After writes, Delta Lake automatically runs a lightweight compaction in the background to keep file sizes optimal.
# MAGIC > These are especially important for a dimension table that receives frequent small MERGE operations.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Initial Load — Seed the Dimension Table

# COMMAND ----------

# MAGIC %md
# MAGIC This simulates the **first-time full load** from the source CRM system.
# MAGIC All records are active (`is_current = True`, `eff_end_date = 9999-12-31`).
# MAGIC In a real pipeline, this data would come from a source extract or a staging table.

# COMMAND ----------

initial_schema = StructType([
    StructField("surrogate_key",  LongType(),      False),
    StructField("customer_id",    IntegerType(),   False),
    StructField("customer_name",  StringType(),    True),
    StructField("email",          StringType(),    True),
    StructField("address",        StringType(),    True),
    StructField("city",           StringType(),    True),
    StructField("country",        StringType(),    True),
    StructField("eff_start_date", StringType(),    False),
    StructField("eff_end_date",   StringType(),    False),
    StructField("is_current",     BooleanType(),   False),
])

initial_data = [
    (1, 101, "Alice Johnson", "alice@retailco.com",  "123 Main St",   "New York",      "USA", "2023-01-01", OPEN_END_DATE, True),
    (2, 102, "Bob Smith",     "bob@retailco.com",    "456 Oak Ave",   "Los Angeles",   "USA", "2023-01-01", OPEN_END_DATE, True),
    (3, 103, "Carol White",   "carol@retailco.com",  "789 Pine Rd",   "Chicago",       "USA", "2023-01-01", OPEN_END_DATE, True),
    (4, 104, "David Brown",   "david@retailco.com",  "321 Elm Blvd",  "Houston",       "USA", "2023-01-01", OPEN_END_DATE, True),
    (5, 105, "Eva Martinez",  "eva@retailco.com",    "555 Cedar Ln",  "Phoenix",       "USA", "2023-01-01", OPEN_END_DATE, True),
]

initial_df = spark.createDataFrame(initial_data, schema=initial_schema) \
    .withColumn("eff_start_date", to_date(col("eff_start_date"))) \
    .withColumn("eff_end_date",   to_date(col("eff_end_date"))) \
    .withColumn("created_at",     current_timestamp()) \
    .withColumn("updated_at",     current_timestamp())

initial_df.write.format("delta").mode("overwrite").saveAsTable(FULL_TABLE_NAME)

print(f"Initial load complete — {initial_df.count()} records written.")
spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} ORDER BY customer_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Simulate Incoming Changes (Staging Data)
# MAGIC
# MAGIC This represents the **daily delta feed** arriving from the CRM system.
# MAGIC In production, this would typically land in a staging table via ADF, Fivetran, Kafka, or a CDC tool (Debezium).
# MAGIC
# MAGIC **Scenarios we're handling:**
# MAGIC
# MAGIC | Customer | Change | SCD2 Action |
# MAGIC |----------|--------|-------------|
# MAGIC | Alice (101) | Moved to Seattle — address + city changed | Expire old row, insert new version |
# MAGIC | Bob (102) | Email address updated | Expire old row, insert new version |
# MAGIC | Eva (105) | Relocated to Miami, also changed email | Expire old row, insert new version |
# MAGIC | Frank (106) | Brand new customer — not in dimension yet | Insert as first active version |
# MAGIC | Carol (103) | No changes | No action — should be ignored |

# COMMAND ----------

staging_schema = StructType([
    StructField("customer_id",    IntegerType(), False),
    StructField("customer_name",  StringType(),  True),
    StructField("email",          StringType(),  True),
    StructField("address",        StringType(),  True),
    StructField("city",           StringType(),  True),
    StructField("country",        StringType(),  True),
])

staging_data = [
    (101, "Alice Johnson", "alice@retailco.com",  "88 Rainier Ave",  "Seattle",     "USA"),  # address+city changed
    (102, "Bob Smith",     "bob.smith@gmail.com", "456 Oak Ave",     "Los Angeles", "USA"),  # email changed
    (103, "Carol White",   "carol@retailco.com",  "789 Pine Rd",     "Chicago",     "USA"),  # no change
    (105, "Eva Martinez",  "eva.m@gmail.com",     "77 Brickell Ave", "Miami",       "USA"),  # email+address+city
    (106, "Frank Lee",     "frank@retailco.com",  "900 Harbor Blvd", "San Diego",   "USA"),  # new customer
]

staging_df = spark.createDataFrame(staging_data, schema=staging_schema)
staging_df.createOrReplaceTempView("staging_updates")

print("Staging data (incoming changes):")
staging_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: SCD Type 2 MERGE — Core Logic
# MAGIC
# MAGIC The MERGE is split into **two deliberate passes**:
# MAGIC
# MAGIC ### Why two passes and not one?
# MAGIC
# MAGIC Delta Lake MERGE supports `WHEN MATCHED UPDATE` and `WHEN NOT MATCHED INSERT` in a single statement,
# MAGIC but SCD2 requires a subtlety:
# MAGIC
# MAGIC - For a **changed record**, you need to:
# MAGIC   1. **Expire** the existing active row (UPDATE `eff_end_date`, `is_current`)
# MAGIC   2. **Insert** a brand new row as the current version
# MAGIC
# MAGIC A single MERGE cannot both UPDATE an existing row AND INSERT a new row for the **same matched key**
# MAGIC in the same pass. Attempting it either silently skips the insert or causes ambiguity.
# MAGIC
# MAGIC The clean, production-safe approach: **two sequential atomic operations**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pass 1 — Expire changed records
# MAGIC
# MAGIC Find all active rows in `customer_dim` where the incoming data has a different `address`, `city`, or `email`.
# MAGIC Mark those rows as expired: set `eff_end_date = yesterday` and `is_current = false`.
# MAGIC
# MAGIC > We use `current_date() - 1` as the end date so that the new version's start date (`current_date()`)
# MAGIC > and the old version's end date don't overlap — keeping the date ranges clean and non-overlapping.

# COMMAND ----------

expire_merge = spark.sql(f"""
    MERGE INTO {FULL_TABLE_NAME} AS target
    USING staging_updates AS source
    ON  target.customer_id = source.customer_id
    AND target.is_current  = true
    AND (
        target.address <> source.address
        OR target.city  <> source.city
        OR target.email <> source.email
    )
    WHEN MATCHED THEN UPDATE SET
        target.eff_end_date = DATE_SUB(CURRENT_DATE(), 1),
        target.is_current   = false,
        target.updated_at   = CURRENT_TIMESTAMP()
""")

print("Pass 1 complete — expired changed records.")
print(f"Rows affected: {expire_merge}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pass 2 — Insert new active versions
# MAGIC
# MAGIC Now insert a fresh row for every incoming record that **does not currently have an active match**
# MAGIC in `customer_dim`. This covers two cases:
# MAGIC
# MAGIC 1. Records we just expired in Pass 1 — they no longer have `is_current = true`, so they'll be inserted fresh
# MAGIC 2. Genuinely new customers who have never been in the dimension before
# MAGIC
# MAGIC > **Surrogate key generation:** In production, use a **sequence/identity column** or a **UUID**.
# MAGIC > Here we derive it as `MAX(surrogate_key) + row_number()` to keep it simple and collision-free for the demo.
# MAGIC > On Databricks Unity Catalog, `GENERATED ALWAYS AS IDENTITY` is the preferred approach.

# COMMAND ----------

# Build the new rows to insert — join staging with current dim to generate surrogate keys
max_sk = spark.sql(f"SELECT COALESCE(MAX(surrogate_key), 0) AS max_sk FROM {FULL_TABLE_NAME}").collect()[0]["max_sk"]

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id

new_versions_df = spark.sql(f"""
    SELECT
        s.customer_id,
        s.customer_name,
        s.email,
        s.address,
        s.city,
        s.country
    FROM staging_updates s
    LEFT JOIN {FULL_TABLE_NAME} t
        ON  s.customer_id = t.customer_id
        AND t.is_current  = true
    WHERE t.customer_id IS NULL
""")

# Assign surrogate keys sequentially
window_spec = Window.orderBy(monotonically_increasing_id())
new_versions_df = new_versions_df \
    .withColumn("row_num",        row_number().over(window_spec)) \
    .withColumn("surrogate_key",  (col("row_num") + lit(max_sk)).cast(LongType())) \
    .withColumn("eff_start_date", current_date()) \
    .withColumn("eff_end_date",   to_date(lit(OPEN_END_DATE))) \
    .withColumn("is_current",     lit(True)) \
    .withColumn("created_at",     current_timestamp()) \
    .withColumn("updated_at",     current_timestamp()) \
    .drop("row_num")

print(f"New rows to insert: {new_versions_df.count()}")
new_versions_df.show(truncate=False)

new_versions_df.write.format("delta").mode("append").saveAsTable(FULL_TABLE_NAME)
print("Pass 2 complete — inserted new active versions.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Validate the Results

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a — Full dimension table after SCD2 processing

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     surrogate_key,
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     email,
# MAGIC     address,
# MAGIC     city,
# MAGIC     eff_start_date,
# MAGIC     eff_end_date,
# MAGIC     is_current
# MAGIC FROM retail.customer_dim
# MAGIC ORDER BY customer_id, eff_start_date;

# COMMAND ----------

# MAGIC %md
# MAGIC **What to observe:**
# MAGIC - Alice (101), Bob (102), Eva (105): two rows each — one expired, one active
# MAGIC - Carol (103): still one row — no change detected, untouched
# MAGIC - Frank (106): one new row — inserted as new customer
# MAGIC - David (104): still one row — not in staging, untouched

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b — Current active customers only (what BI tools query)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, customer_name, email, city, eff_start_date
# MAGIC FROM retail.customer_dim
# MAGIC WHERE is_current = true
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c — Point-in-time query
# MAGIC
# MAGIC Answer: *"Where did Alice live on 2023-06-15?"*
# MAGIC
# MAGIC This is the fundamental value of SCD2 — you can reconstruct the state of the dimension at any past date.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     address,
# MAGIC     city,
# MAGIC     eff_start_date,
# MAGIC     eff_end_date,
# MAGIC     is_current
# MAGIC FROM retail.customer_dim
# MAGIC WHERE customer_id = 101
# MAGIC   AND eff_start_date <= '2023-06-15'
# MAGIC   AND eff_end_date   >= '2023-06-15';
# MAGIC -- Expected: 123 Main St, New York — her address before she moved

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5d — Data quality checks
# MAGIC
# MAGIC In production, these assertions run after every SCD2 load as part of a data quality framework (Great Expectations, dbt tests, etc.).
# MAGIC Here we implement them manually to show the intent.

# COMMAND ----------

print("=== Data Quality Checks ===\n")

total         = spark.sql(f"SELECT COUNT(*) AS cnt FROM {FULL_TABLE_NAME}").collect()[0]["cnt"]
active        = spark.sql(f"SELECT COUNT(*) AS cnt FROM {FULL_TABLE_NAME} WHERE is_current = true").collect()[0]["cnt"]
expired       = spark.sql(f"SELECT COUNT(*) AS cnt FROM {FULL_TABLE_NAME} WHERE is_current = false").collect()[0]["cnt"]
duplicates    = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM (
        SELECT customer_id
        FROM {FULL_TABLE_NAME}
        WHERE is_current = true
        GROUP BY customer_id
        HAVING COUNT(*) > 1
    )
""").collect()[0]["cnt"]
open_end_mismatch = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {FULL_TABLE_NAME}
    WHERE is_current = true AND eff_end_date <> DATE('{OPEN_END_DATE}')
""").collect()[0]["cnt"]

print(f"Total rows              : {total}")
print(f"Active rows (is_current): {active}")
print(f"Expired rows            : {expired}")
print(f"[CHECK] Duplicate active keys   : {duplicates}  {'✓ PASS' if duplicates == 0 else '✗ FAIL'}")
print(f"[CHECK] Active rows end date OK : {open_end_mismatch}  {'✓ PASS' if open_end_mismatch == 0 else '✗ FAIL'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Delta Lake Time Travel — Free Audit Trail
# MAGIC
# MAGIC Every MERGE and write on a Delta table is a **versioned transaction**.
# MAGIC Delta's transaction log records who did what and when — no separate audit table needed.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY retail.customer_dim;

# COMMAND ----------

# MAGIC %md
# MAGIC You can also **query a previous version of the table** directly:
# MAGIC
# MAGIC ```sql
# MAGIC -- State of the table before any SCD2 processing (version 1 = after initial load)
# MAGIC SELECT * FROM retail.customer_dim VERSION AS OF 1;
# MAGIC
# MAGIC -- Or by timestamp
# MAGIC SELECT * FROM retail.customer_dim TIMESTAMP AS OF '2024-01-01 00:00:00';
# MAGIC ```
# MAGIC
# MAGIC This is one of Delta Lake's most powerful features for a data warehouse — you get time travel
# MAGIC across the entire table without building a separate history/audit layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Wrapping Up
# MAGIC
# MAGIC Getting SCD2 right the first time is harder than it looks. The logic seems straightforward — expire the old row, insert the new one — but the moment you try to do both in a single MERGE statement, you hit a wall. That two-pass approach isn't a workaround, it's the correct way to think about it: first close what changed, then open fresh versions.
# MAGIC
# MAGIC A few things I'd call out as genuinely easy to get wrong:
# MAGIC
# MAGIC **The surrogate key.** Using `customer_id` as the primary key breaks as soon as you have two rows for the same customer. This is the kind of bug that doesn't blow up immediately — it shows up weeks later when your joins start returning duplicates in reports.
# MAGIC
# MAGIC **NULL vs. `9999-12-31`.** Using NULL for open-ended records feels cleaner at first, but you end up writing `WHERE eff_end_date IS NULL OR eff_end_date >= :query_date` everywhere. The sentinel date keeps queries readable and consistent.
# MAGIC
# MAGIC **`is_current` feels redundant but isn't.** You already have `eff_end_date = '9999-12-31'` to identify active rows, so why add `is_current`? Because most BI tools and analysts filtering for "current customers" shouldn't need to know what the sentinel date convention is. It's a usability decision as much as a performance one.
# MAGIC
# MAGIC **The end date is `current_date - 1`, not `current_date`.** Subtle, but important. If the old version ends today and the new version also starts today, you have a one-day overlap. That breaks point-in-time queries for today's date. Closing yesterday keeps the ranges clean.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Things to think about before taking this to production
# MAGIC
# MAGIC - What happens if the same customer appears twice in the same staging batch? You need deduplication upstream or the MERGE will behave unpredictably.
# MAGIC - How do you handle late-arriving data — a change that should have been effective 3 days ago? The effective dates need to come from the source, not `current_date()`.
# MAGIC - At scale (100M+ customers), partition this table by `country` or a hash bucket of `customer_id`. Add a Z-order on `customer_id` so MERGE scans stay fast.
# MAGIC - Delta's `DESCRIBE HISTORY` gives you a full audit trail out of the box. Before building a separate change log table, check if time travel already covers your compliance requirement.
