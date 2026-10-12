# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Andina Market — control inicial del proyecto
# MAGIC
# MAGIC Este notebook solo inspecciona el entorno. No crea, modifica ni elimina tablas.

# COMMAND ----------

from datetime import datetime, timezone
import importlib.metadata as metadata
import sys

CATALOG = "andina_market_lhcl_prod"
SOURCE_SCHEMA = "rdv_retail"

HISTORICAL_TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "support_tickets",
    "app_events",
]

DRIFT_TABLES = [
    "products_month_13",
    "orders_month_13",
    "order_items_month_13",
    "payments_month_13",
    "support_tickets_month_13",
    "app_events_month_13",
]

print(f"UTC inspection time: {datetime.now(timezone.utc).isoformat()}")
print(f"Python: {sys.version.split()[0]}")
print(f"Spark: {spark.version}")

for package in ["mlflow", "databricks-feature-engineering", "scikit-learn"]:
    try:
        print(f"{package}: {metadata.version(package)}")
    except metadata.PackageNotFoundError:
        print(f"{package}: NOT INSTALLED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contexto efectivo de Unity Catalog

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
          current_catalog() AS current_catalog,
          current_schema() AS current_schema,
          current_user() AS current_user,
          '{CATALOG}' AS expected_catalog,
          '{SOURCE_SCHEMA}' AS expected_source_schema
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Existencia y conteos de tablas esperadas

# COMMAND ----------

inventory = []
for table_name in HISTORICAL_TABLES + DRIFT_TABLES:
    full_name = f"{CATALOG}.{SOURCE_SCHEMA}.{table_name}"
    exists = spark.catalog.tableExists(full_name)
    row_count = spark.table(full_name).count() if exists else None
    inventory.append((table_name, full_name, exists, row_count))

inventory_df = spark.createDataFrame(
    inventory,
    "table_name STRING, full_name STRING, exists BOOLEAN, row_count LONG",
)
display(inventory_df.orderBy("table_name"))

missing_tables = [row[0] for row in inventory if not row[2]]
assert not missing_tables, f"Missing expected tables: {missing_tables}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Esquemas reales

# COMMAND ----------

for table_name in HISTORICAL_TABLES + DRIFT_TABLES:
    full_name = f"{CATALOG}.{SOURCE_SCHEMA}.{table_name}"
    print(f"\n=== {full_name} ===")
    spark.table(full_name).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rangos temporales críticos

# COMMAND ----------

time_checks = spark.sql(
    f"""
    SELECT 'orders' AS source, MIN(order_ts) AS min_ts, MAX(order_ts) AS max_ts
    FROM {CATALOG}.{SOURCE_SCHEMA}.orders
    UNION ALL
    SELECT 'orders_month_13', MIN(order_ts), MAX(order_ts)
    FROM {CATALOG}.{SOURCE_SCHEMA}.orders_month_13
    UNION ALL
    SELECT 'app_events', MIN(event_ts), MAX(event_ts)
    FROM {CATALOG}.{SOURCE_SCHEMA}.app_events
    UNION ALL
    SELECT 'app_events_month_13', MIN(event_ts), MAX(event_ts)
    FROM {CATALOG}.{SOURCE_SCHEMA}.app_events_month_13
    """
)
display(time_checks)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integridad referencial básica

# COMMAND ----------

integrity_checks = spark.sql(
    f"""
    SELECT 'orders_without_customer' AS check_name, COUNT(*) AS violations
    FROM {CATALOG}.{SOURCE_SCHEMA}.orders o
    LEFT ANTI JOIN {CATALOG}.{SOURCE_SCHEMA}.customers c
      ON o.customer_id = c.customer_id
    UNION ALL
    SELECT 'items_without_order', COUNT(*)
    FROM {CATALOG}.{SOURCE_SCHEMA}.order_items i
    LEFT ANTI JOIN {CATALOG}.{SOURCE_SCHEMA}.orders o
      ON i.order_id = o.order_id
    UNION ALL
    SELECT 'items_without_product', COUNT(*)
    FROM {CATALOG}.{SOURCE_SCHEMA}.order_items i
    LEFT ANTI JOIN {CATALOG}.{SOURCE_SCHEMA}.products p
      ON i.sku = p.sku
    UNION ALL
    SELECT 'payments_without_order', COUNT(*)
    FROM {CATALOG}.{SOURCE_SCHEMA}.payments p
    LEFT ANTI JOIN {CATALOG}.{SOURCE_SCHEMA}.orders o
      ON p.order_id = o.order_id
    """
)
display(integrity_checks)

violations = sum(row["violations"] for row in integrity_checks.collect())
assert violations == 0, f"Referential integrity violations found: {violations}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comprobación de schemas objetivo
# MAGIC
# MAGIC Solo consulta cuáles existen; no intenta crearlos.

# COMMAND ----------

available_schemas = spark.sql(f"SHOW SCHEMAS IN {CATALOG}")
display(available_schemas)

target_schemas = {"features", "models", "monitoring"}
schema_column = available_schemas.columns[0]
existing_schemas = {row[schema_column] for row in available_schemas.collect()}
print("Existing target schemas:", sorted(target_schemas & existing_schemas))
print("Missing target schemas:", sorted(target_schemas - existing_schemas))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado esperado
# MAGIC
# MAGIC - Las 13 tablas existen.
# MAGIC - Los controles de integridad muestran cero violaciones.
# MAGIC - Los datos históricos y el mes 13 tienen periodos separados.
# MAGIC - Las versiones instaladas quedan registradas antes de escribir el Bloque 1.
# MAGIC
# MAGIC Entregue estos outputs a Genie Code y pídale continuar desde `CURRENT_STATE.md`.