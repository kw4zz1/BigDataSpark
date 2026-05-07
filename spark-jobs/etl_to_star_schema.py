from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, dayofmonth, month, year, quarter, dayofweek, weekofyear, date_format, when
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

def create_spark_session():
    return SparkSession.builder \
        .appName("ETL to Star Schema") \
        .master("spark://spark-master:7077") \
        .config("spark.jars", "/opt/spark-jars/postgresql-42.7.1.jar") \
        .getOrCreate()

def load_staging_data(spark):
    return spark.read.jdbc(url=POSTGRES_URL, table="staging_data", properties=POSTGRES_PROPERTIES)

def transform_dim_customer(staging_df):
    return staging_df.select(
        col("sale_customer_id").alias("customer_id"),
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_age").alias("age"),
        col("customer_email").alias("email"),
        col("customer_country").alias("country"),
        col("customer_postal_code").alias("postal_code"),
        col("customer_pet_type").alias("pet_type"),
        col("customer_pet_name").alias("pet_name"),
        col("customer_pet_breed").alias("pet_breed")
    ).dropDuplicates(["customer_id"])

def transform_dim_seller(staging_df):
    return staging_df.select(
        col("sale_seller_id").alias("seller_id"),
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("seller_email").alias("email"),
        col("seller_country").alias("country"),
        col("seller_postal_code").alias("postal_code")
    ).dropDuplicates(["seller_id"])

def transform_dim_product(staging_df):
    return staging_df.select(
        col("sale_product_id").alias("product_id"),
        col("product_name").alias("name"),
        col("product_category").alias("category"),
        col("product_price").alias("price"),
        col("product_weight").alias("weight"),
        col("product_color").alias("color"),
        col("product_size").alias("size"),
        col("product_brand").alias("brand"),
        col("product_material").alias("material"),
        col("product_description").alias("description"),
        col("product_rating").alias("rating"),
        col("product_reviews").alias("reviews"),
        to_date(col("product_release_date"), "M/d/yyyy").alias("release_date"),
        to_date(col("product_expiry_date"), "M/d/yyyy").alias("expiry_date"),
        col("pet_category")
    ).dropDuplicates(["product_id"])

def transform_dim_store(staging_df):
    return staging_df.select(
        col("store_name").alias("name"),
        col("store_location").alias("location"),
        col("store_city").alias("city"),
        col("store_state").alias("state"),
        col("store_country").alias("country"),
        col("store_phone").alias("phone"),
        col("store_email").alias("email")
    ).dropDuplicates(["name", "city", "country"])

def transform_dim_supplier(staging_df):
    return staging_df.select(
        col("supplier_name").alias("name"),
        col("supplier_contact").alias("contact"),
        col("supplier_email").alias("email"),
        col("supplier_phone").alias("phone"),
        col("supplier_address").alias("address"),
        col("supplier_city").alias("city"),
        col("supplier_country").alias("country")
    ).dropDuplicates(["name", "email"])

def transform_dim_date(staging_df):
    date_df = staging_df.select(to_date(col("sale_date"), "M/d/yyyy").alias("date")).dropDuplicates()
    return date_df.select(
        col("date"),
        dayofmonth(col("date")).alias("day"),
        month(col("date")).alias("month"),
        year(col("date")).alias("year"),
        quarter(col("date")).alias("quarter"),
        dayofweek(col("date")).alias("day_of_week"),
        weekofyear(col("date")).alias("week_of_year"),
        date_format(col("date"), "MMMM").alias("month_name"),
        when(dayofweek(col("date")).isin([1, 7]), True).otherwise(False).alias("is_weekend")
    )

def write_to_postgres(df, table_name):
    # Truncate via a dummy read with sessionInitStatement, then append
    # This preserves the SERIAL/sequence columns defined in the SQL schema
    truncate_props = dict(POSTGRES_PROPERTIES)
    truncate_props["sessionInitStatement"] = f"TRUNCATE TABLE {table_name}"
    spark = SparkSession.getActiveSession()
    try:
        spark.read.jdbc(
            url=POSTGRES_URL,
            table="(SELECT 1 LIMIT 0) AS t",
            properties=truncate_props
        ).count()
    except Exception:
        pass
    df.write.jdbc(url=POSTGRES_URL, table=table_name, mode="append", properties=POSTGRES_PROPERTIES)

def load_from_postgres(spark, table_name):
    return spark.read.jdbc(url=POSTGRES_URL, table=table_name, properties=POSTGRES_PROPERTIES)

def transform_fact_sales(spark, staging_df):
    dim_customer = load_from_postgres(spark, "dim_customer")
    dim_seller = load_from_postgres(spark, "dim_seller")
    dim_product = load_from_postgres(spark, "dim_product")
    dim_store = load_from_postgres(spark, "dim_store")
    dim_supplier = load_from_postgres(spark, "dim_supplier")
    dim_date = load_from_postgres(spark, "dim_date")

    staging_prep = staging_df.select(
        col("sale_customer_id"),
        col("sale_seller_id"),
        col("sale_product_id"),
        col("store_name"),
        col("store_city"),
        col("store_country"),
        col("supplier_name"),
        col("supplier_email"),
        to_date(col("sale_date"), "M/d/yyyy").alias("sale_date"),
        col("sale_quantity").alias("quantity"),
        col("sale_total_price").alias("total_price"),
        col("product_price").alias("unit_price")
    )

    fact_df = staging_prep \
        .join(dim_customer, staging_prep.sale_customer_id == dim_customer.customer_id, "left") \
        .join(dim_seller, staging_prep.sale_seller_id == dim_seller.seller_id, "left") \
        .join(dim_product, staging_prep.sale_product_id == dim_product.product_id, "left") \
        .join(dim_store, (staging_prep.store_name == dim_store.name) &
              (staging_prep.store_city == dim_store.city) &
              (staging_prep.store_country == dim_store.country), "left") \
        .join(dim_supplier, (staging_prep.supplier_name == dim_supplier.name) &
              (staging_prep.supplier_email == dim_supplier.email), "left") \
        .join(dim_date, staging_prep.sale_date == dim_date.date, "left")

    return fact_df.select(
        dim_customer.customer_id,
        dim_seller.seller_id,
        dim_product.product_id,
        dim_store.store_id,
        dim_supplier.supplier_id,
        dim_date.date_id,
        col("quantity"),
        col("total_price"),
        col("unit_price")
    )

def main():
    spark = create_spark_session()

    try:
        staging_df = load_staging_data(spark)
        staging_df.cache()

        write_to_postgres(transform_dim_customer(staging_df), "dim_customer")
        write_to_postgres(transform_dim_seller(staging_df), "dim_seller")
        write_to_postgres(transform_dim_product(staging_df), "dim_product")
        write_to_postgres(transform_dim_store(staging_df), "dim_store")
        write_to_postgres(transform_dim_supplier(staging_df), "dim_supplier")
        write_to_postgres(transform_dim_date(staging_df), "dim_date")
        write_to_postgres(transform_fact_sales(spark, staging_df), "fact_sales")

        print("ETL to Star Schema completed")
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
