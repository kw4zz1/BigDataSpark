from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, desc
import os

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_URL = f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@mongodb:27017/{MONGO_DB}?authSource=admin"

def create_spark_session():
    return SparkSession.builder \
        .appName("ETL to MongoDB") \
        .master("spark://spark-master:7077") \
        .config("spark.jars", "/opt/spark-jars/postgresql-42.7.1.jar,/opt/spark-jars/mongo-spark-connector_2.12-10.3.0.jar") \
        .config("spark.mongodb.write.connection.uri", MONGO_URI) \
        .getOrCreate()

def load_table(spark, table_name):
    return spark.read.jdbc(url=POSTGRES_URL, table=table_name, properties=POSTGRES_PROPERTIES)

def write_to_mongodb(df, collection_name):
    df.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("connection.uri", MONGO_URI) \
        .option("database", MONGO_DB) \
        .option("collection", collection_name) \
        .save()

def create_report_products(fact_sales, dim_product):
    return fact_sales.join(dim_product, "product_id").groupBy(
        col("product_id"),
        col("name").alias("product_name"),
        col("category"),
        col("rating"),
        col("reviews")
    ).agg(
        _sum("quantity").alias("total_quantity_sold"),
        _sum("total_price").alias("total_revenue"),
        count("*").alias("number_of_sales")
    ).orderBy(desc("total_quantity_sold"))

def create_report_customers(fact_sales, dim_customer):
    return fact_sales.join(dim_customer, "customer_id").groupBy(
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        col("email"),
        col("country"),
        col("age")
    ).agg(
        _sum("total_price").alias("total_spent"),
        count("*").alias("number_of_purchases"),
        avg("total_price").alias("average_check")
    ).orderBy(desc("total_spent"))

def create_report_time(fact_sales, dim_date):
    return fact_sales.join(dim_date, "date_id").groupBy(
        col("year"),
        col("month"),
        col("month_name"),
        col("quarter")
    ).agg(
        _sum("total_price").alias("total_revenue"),
        _sum("quantity").alias("total_quantity"),
        count("*").alias("number_of_sales"),
        avg("total_price").alias("average_order_size")
    ).orderBy("year", "month")

def create_report_stores(fact_sales, dim_store):
    return fact_sales.join(dim_store, "store_id").groupBy(
        col("store_id"),
        col("name").alias("store_name"),
        col("city"),
        col("country"),
        col("phone"),
        col("email")
    ).agg(
        _sum("total_price").alias("total_revenue"),
        count("*").alias("number_of_sales"),
        avg("total_price").alias("average_check")
    ).orderBy(desc("total_revenue"))

def create_report_suppliers(fact_sales, dim_supplier, dim_product):
    return fact_sales.join(dim_supplier, "supplier_id").join(dim_product, "product_id").groupBy(
        col("supplier_id"),
        dim_supplier.name.alias("supplier_name"),
        dim_supplier.country.alias("supplier_country"),
        col("contact"),
        col("phone")
    ).agg(
        _sum("total_price").alias("total_revenue"),
        count("*").alias("number_of_sales"),
        avg(dim_product.price).alias("average_product_price")
    ).orderBy(desc("total_revenue"))

def create_report_quality(fact_sales, dim_product):
    return fact_sales.join(dim_product, "product_id").groupBy(
        col("product_id"),
        col("name").alias("product_name"),
        col("category"),
        col("rating"),
        col("reviews"),
        col("brand")
    ).agg(
        _sum("quantity").alias("total_quantity_sold"),
        _sum("total_price").alias("total_revenue"),
        count("*").alias("number_of_sales")
    ).orderBy(desc("rating"), desc("reviews"))

def main():
    spark = create_spark_session()

    try:
        fact_sales = load_table(spark, "fact_sales")
        dim_customer = load_table(spark, "dim_customer")
        dim_product = load_table(spark, "dim_product")
        dim_store = load_table(spark, "dim_store")
        dim_supplier = load_table(spark, "dim_supplier")
        dim_date = load_table(spark, "dim_date")

        write_to_mongodb(create_report_products(fact_sales, dim_product), "report_products")
        write_to_mongodb(create_report_customers(fact_sales, dim_customer), "report_customers")
        write_to_mongodb(create_report_time(fact_sales, dim_date), "report_time")
        write_to_mongodb(create_report_stores(fact_sales, dim_store), "report_stores")
        write_to_mongodb(create_report_suppliers(fact_sales, dim_supplier, dim_product), "report_suppliers")
        write_to_mongodb(create_report_quality(fact_sales, dim_product), "report_quality")

        print("MongoDB reports created")
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
