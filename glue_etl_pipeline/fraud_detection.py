import sys
import boto3
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from datetime import datetime
import logging


def getSparkContext():
    # Parse job arguments
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET_PATH', 'INPUT_DB','OUTPUT_DB'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    s3_output_path = args['S3_TARGET_PATH'] + args["JOB_NAME"]
    bronze_db = args['INPUT_DB']
    output_db = args['OUTPUT_DB']

    # Initialize Logger
    logger =  logging.getLogger("glue_etl_pipeline")
    logger.setLevel(logging.INFO)

    return spark, job, args, s3_output_path, bronze_db,output_db, logger


def run_etl():
    spark, job, args, s3_output_path, bronze_db,output_db, logger = getSparkContext()

    try:
        # Read source tables
        orders_df = spark.read.table(f"{bronze_db}.orders")
        user_logins_df = spark.read.table(f"{bronze_db}.loginhistory")

        # Register DataFrames as temporary views for Spark SQL
        user_logins_df.createOrReplaceTempView("LoginHistory")
        orders_df.createOrReplaceTempView("Orders")

        # Common transformation (choose SQL or DataFrame version)
        high_risk_customers = transform_sql(spark)
        # high_risk_customers = transform_dataframe(orders_df, user_logins_df)

        # Write results to S3
        #write_to_s3(high_risk_customers, s3_output_path)
        write_to_s3_create_table(high_risk_customers,s3_output_path,output_db,args["JOB_NAME"])

        print("ETL Job Completed Successfully")

    except Exception as e:
        print(f"ETL Job Failed: {str(e)}")
        raise e

    finally:
        job.commit()


def transform_sql(spark):
    query = """
    WITH suspicious_logins AS (
        SELECT
            customer_id,
            COUNT(DISTINCT ip_address) AS unique_ips,
            COUNT(*) AS total_attempts
        FROM LoginHistory
        WHERE login_date >= date_add(current_date(), -30)
        GROUP BY customer_id
        HAVING unique_ips > 3 OR total_attempts > 10
    ),
    high_risk_orders AS (
        SELECT
            customer_id,
            COUNT(order_id) AS order_count,
            SUM(total_amount) AS total_spent
        FROM Orders
        WHERE order_date >= date_add(current_date(), -30)
        GROUP BY customer_id
        HAVING total_spent > 5000 OR order_count > 5
    )
    SELECT DISTINCT s.customer_id
    FROM suspicious_logins s
    JOIN high_risk_orders h ON s.customer_id = h.customer_id
    """

    high_risk_customers = spark.sql(query)
    high_risk_customers.show()
    return high_risk_customers


def transform_dataframe(order_df, user_logins_df):
    # Define the date range (last 30 days)
    date_threshold = F.date_add(F.current_date(), -30)

    # Identify suspicious logins
    suspicious_logins = (
        user_logins_df.filter(F.col("login_date") >= date_threshold)
        .groupBy("customer_id")
        .agg(
            F.countDistinct("ip_address").alias("unique_ips"),
            F.count("*").alias("total_attempts")
        )
        .filter((F.col("unique_ips") > 3) | (F.col("total_attempts") > 10))
    )

    # Identify high-risk orders
    high_risk_orders = (
        order_df.filter(F.col("order_date") >= date_threshold)
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("total_amount").alias("total_spent")
        )
        .filter((F.col("total_spent") > 5000) | (F.col("order_count") > 5))
    )

    suspicious_logins.show()
    high_risk_orders.show()

    # Join both datasets
    suspicious_customers_df = suspicious_logins.join(
        high_risk_orders,
        on="customer_id",
        how="inner"
    ).select("customer_id").distinct()

    suspicious_customers_df.show()
    return suspicious_customers_df


def write_to_s3_create_table(df: DataFrame,s3_path: str,output_db: str, tableName: str, format="parquet", mode="overwrite"):
    print(f"Write data to S3 Started: {s3_path}")
    df.show(10)
    print(df.count())
    df.write.mode(mode).format(format).save(s3_path)
    df.write.format(format) .mode(mode) .option("path", s3_path).saveAsTable(f"{output_db}.{tableName}")
    print(f"Write data to S3 Completed: {s3_path}")


if __name__ == "__main__":
    run_etl()