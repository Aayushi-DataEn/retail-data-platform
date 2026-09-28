from delta.tables import DeltaTable
from pyspark.sql import SparkSession


def scd1(spark, source_df, key_columns, target_table):

    spark = SparkSession.getActiveSession()

    if spark is None:
        raise RuntimeError("No active Spark session found.")

    if spark.catalog.tableExists(target_table):

        print("Table exists. Processing SCD Type 1.")

        target = DeltaTable.forName(
            spark,
            target_table
        )

        # Build merge condition
        merge_condition = " AND ".join(
            [
                f"target.{column} = source.{column}"
                for column in key_columns
            ]
        )

        print(f"SCD Type 1 merge condition: {merge_condition}")

        (
            target.alias("target")
            .merge(
                source_df.alias("source"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("SCD Type 1 completed successfully.")

    else:

        print("Table does not exist. Creating table.")

        catalog, schema, table = target_table.split(".")

        spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
        """)

        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )

        print("Table created successfully.")