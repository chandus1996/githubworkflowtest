import argparse

from pyspark.sql import SparkSession, functions as F


def run(input_path: str, output_path: str) -> None:
    spark = SparkSession.builder.appName("workflowtest-pyspark-job").getOrCreate()

    customers_df = (
        spark.read.option("header", True)
        .csv(f"{input_path}/customers_*.csv")
        .select(
            F.col("customer_id").cast("int"),
            F.col("customer_name"),
            F.col("state"),
        )
    )

    orders_df = (
        spark.read.option("header", True)
        .csv(f"{input_path}/orders_*.csv")
        .select(
            F.col("order_id").cast("int"),
            F.col("customer_id").cast("int"),
            F.col("amount").cast("double"),
            F.col("order_date"),
        )
    )

    filtered_orders_df = orders_df.filter(F.col("amount") >= 200)

    enriched_df = (
        filtered_orders_df.join(customers_df, on="customer_id", how="inner")
        .withColumn("amount_with_tax", F.round(F.col("amount") * F.lit(1.18), 2))
        .withColumn("order_month", F.date_format(F.to_date("order_date"), "yyyy-MM"))
        .withColumn("ingestion_ts", F.current_timestamp())
    )

    (
        enriched_df.orderBy("order_id")
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )

    spark.stop()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run workflowtest PySpark ETL job")
    parser.add_argument(
        "--input-path",
        default="data/input",
        help="Input folder containing 10 CSV files",
    )
    parser.add_argument(
        "--output-path",
        default="data/output/processed_orders",
        help="Output folder for transformed CSV",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(args.input_path, args.output_path)
