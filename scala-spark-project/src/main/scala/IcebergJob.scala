import org.apache.spark.sql.SparkSession

object IcebergJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IcebergScalaExample")
      .master("local[*]")
      .config("spark.sql.catalog.mysql", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.mysql.type", "jdbc")
      .config("spark.sql.catalog.mysql.uri", "jdbc:mysql://mysql:3306/exampledb")
      .config("spark.sql.catalog.mysql.jdbc.user", "exampleuser")
      .config("spark.sql.catalog.mysql.jdbc.password", "examplepass")
      .config("spark.sql.catalog.mysql.jdbc.driver", "com.mysql.cj.jdbc.Driver")
      .config("spark.sql.catalog.mysql.warehouse", "hdfs://namenode:9000/user/hive/warehouse")
      .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()

    spark.sql("CREATE TABLE IF NOT EXISTS mysql.test_table (id INT, name STRING) USING iceberg")
    spark.sql("INSERT INTO mysql.test_table VALUES (1, 'Alice'), (2, 'Bob')")

    val df = spark.sql("SELECT * FROM mysql.test_table")
    df.show()

    spark.stop()
  }
}
