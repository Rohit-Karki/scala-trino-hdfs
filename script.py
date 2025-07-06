from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("IcebergWithPySpark")
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2,"
        "org.postgresql:postgresql:42.6.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.default", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.default.type", "jdbc")
    .config(
        "spark.sql.catalog.default.uri", "jdbc:postgresql://postgres:5432/exampledb"
    )
    .config("spark.sql.catalog.default.jdbc.user", "exampleuser")
    .config("spark.sql.catalog.default.jdbc.password", "examplepass")
    .config("spark.sql.catalog.default.jdbc.driver", "org.postgresql.Driver")
    .config(
        "spark.sql.catalog.default.warehouse",
        "hdfs://namenode:9000/user/hive/warehouse",
    )
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)

# Example action to test everything is connected
spark.sql("SHOW TABLES").show()
