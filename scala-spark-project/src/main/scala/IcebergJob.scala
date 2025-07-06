import org.apache.spark.sql.SparkSession

object IcebergJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IcebergScalaExample")
      .master("local[*]")
      .config("spark.sql.catalog.default", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.default.type", "jdbc")
      .config("spark.sql.catalog.default.uri", "jdbc:postgresql://postgres:5432/exampledb")
      .config("spark.sql.catalog.default.jdbc.user", "exampleuser")
      .config("spark.sql.catalog.default.jdbc.password", "examplepass")
      .config("spark.sql.catalog.default.jdbc.driver", "org.postgresql.Driver")
      .config("spark.sql.catalog.default.warehouse", "hdfs://namenode:9000/user/hive/warehouse")
      .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()

    spark.sql("CREATE TABLE IF NOT EXISTS default.test_table (id INT, name STRING) USING iceberg")
    spark.sql("INSERT INTO default.test_table VALUES (1, 'Alice'), (2, 'Bob')")

    val df = spark.sql("SELECT * FROM default.test_table")
    df.show()

    spark.stop()
  }
}
