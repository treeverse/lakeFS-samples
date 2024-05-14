package example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, to_date, to_timestamp, lit}

object etl_jobs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    println(args.mkString(", "))

    val ENVIRONMENT = args(0)
    val lakefs_repo = args(1)
    val lakefs_branch = args(2)
    val data_source_storage_namespace = args(3)
    val PROD_DATA_SOURCE = data_source_storage_namespace
    val DEV_DATA_SOURCE = s"lakefs://${lakefs_repo}/${lakefs_branch}/delta-tables"

    val DATA_SOURCE: String = if (ENVIRONMENT == "prod") PROD_DATA_SOURCE else (if (ENVIRONMENT == "dev") DEV_DATA_SOURCE else "")
    println(DATA_SOURCE)

    etl_job(spark, DATA_SOURCE)
  }

  def etl_job(spark: SparkSession, DATA_SOURCE: String) = {
    val df = spark.read.format("delta").load(s"${DATA_SOURCE}/famous_people_raw")
    df.write.format("delta").mode("overwrite").partitionBy("country").save(s"${DATA_SOURCE}/famous_people")
    df.show()

    //val df_category = spark.read.format("delta").load(s"${DATA_SOURCE}/category_raw")
    //val df_not_music = df_category.filter("category not like 'music'")
    //df_not_music.write.format("delta").mode("overwrite").save(s"${DATA_SOURCE}/category_raw")
    //df_not_music.show()
  }
}
