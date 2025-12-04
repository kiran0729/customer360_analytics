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
    try:
        spark, job, args, s3_output_path, input_db,output_db, logger = getSparkContext()
        start_time = datetime.now()
        print("Staring ETL Job      ---   " +args["JOB_NAME"])

        print(f"{input_db}.customers")

        customer_df = spark.read.table(f"{input_db}.customers")
        order_df = spark.read.table(f"{input_db}.orders")
        customer_df.createOrReplaceTempView("customers")
        order_df.createOrReplaceTempView("orders")

        customer_df.show()
        
        # SQL transformation 
        top_customers=transform_top_customers_sql(spark)
 
        #top_customers=transform_dataframe(order_df,customer_df)

        
        # write  gold catalog 
        write_to_s3_create_table(top_customers,s3_output_path,output_db,args["JOB_NAME"])

        end_time = datetime.now()
        print("ETL Job Completed Successfully")


    except Exception as e:
        end_time = datetime.now()
        print(f"ETL Job Failed: {str(e)}")
        raise e
    job.commit()






def transform_top_customers_sql(spark):
    return spark.sql("""
                WITH customer_spending AS (
                    SELECT
                        o.customer_id,
                        SUM(o.total_amount) AS total_spent,
                        COUNT(o.order_id) AS total_orders,
                        MAX(o.order_date) AS last_purchase_date
                    FROM orders o
                    WHERE o.order_date >= date_add(current_date(), -365)  -- Last 1 year
                    GROUP BY o.customer_id
                ),
                customer_ranking AS (
                    SELECT
                        c.country,
                        CONCAT(c.first_name, ' ', c.last_name) AS full_name,
                        c.email,
                        cs.total_spent,
                        cs.total_orders,
                        cs.last_purchase_date,
                        RANK() OVER (PARTITION BY c.country ORDER BY cs.total_spent DESC) AS spending_rank
                    FROM customer_spending cs
                    JOIN customers c ON cs.customer_id = c.customer_id
                )
                SELECT * FROM customer_ranking WHERE spending_rank <= 10 
                     


    """)



def write_to_s3_create_table(df: DataFrame,s3_path: str,output_db: str, tableName: str, format="parquet", mode="overwrite"):
    print(f"Write data to S3 Started: {s3_path}")
    df.show(10)
    print(df.count())
    df.write.mode(mode).format(format).save(s3_path)
    df.write.format(format) .mode(mode) .option("path", s3_path).saveAsTable(f"{output_db}.{tableName}")
    print(f"Write data to S3 Completed: {s3_path}")



def transform_dataframe(order_df,customer_df):
    # Filter last 1 year of data
    one_year_ago = F.date_add(F.current_date(), -365)
    filtered_orders = order_df.filter(F.col("order_date") >= one_year_ago)

    # Aggregate customer spending
    customer_spending = (
        filtered_orders.groupBy("customer_id")
        .agg(
            F.sum("total_amount").alias("total_spent"),
            F.count("order_id").alias("total_orders"),
            F.max("order_date").alias("last_purchase_date")
        )
    )

    # Join with customers table
    customer_data = customer_spending.join(customer_df, "customer_id")

    # Define window specification for ranking
    window_spec = Window.partitionBy("country").orderBy(F.desc("total_spent"))

    # Add ranking column
    customer_ranking = customer_data.withColumn("spending_rank", F.rank().over(window_spec))

    # Filter top 100 customers
    top_customers = customer_ranking.filter( (F.col("spending_rank") <= 10) &  (F.col("country").like("United %")))

    # Show results
    top_customers.select("country","customer_id","first_name","email","total_spent","total_orders","spending_rank").show(20)

    return top_customers


if __name__ == "__main__":
    run_etl()