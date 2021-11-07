package com.contiamo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.thriftserver._

object Backend {
  def query: Boolean = {
    val spark = SparkSession.builder
      .master("local")
      .appName("Spark SQL Example")
      .config("hive.server2.thrift.port", 10002) // required for thriftserver
      .config("spark.sql.hive.thriftServer.singleSession", true) // required for thriftserver
      .enableHiveSupport // required for thriftserver
      .getOrCreate

    val jdbcDF = spark.read
      .format("jdbc")
      .options(Map("url" -> "jdbc:postgresql://localhost:5432/article_performance_666570051",
                   "dbtable" -> "public.article_cost_facts2"))
      .load
    jdbcDF.createOrReplaceTempView("articles")

    val jdbc2DF = spark.read
      .format("jdbc")
      .options(
        Map("url" -> "jdbc:postgresql://localhost:5432/article_performance_666570051", "dbtable" -> "public.dates"))
      .load

    jdbc2DF.show(5)
//    jdbc2DF.write.mode("overwrite").saveAsTable("dates_saved")
    jdbc2DF.createOrReplaceTempView("dates")

    val result = spark.sql(
      "SELECT d.year, d.day_of_month, a.article_id FROM articles as a JOIN dates as d on a.date_key = d.key where a.article_id > 96000 limit 10")
    result.show()

    val cat = spark.catalog
    cat.listTables("default").show()
    cat.listDatabases().show()

    HiveThriftServer2.startWithContext(spark.sqlContext)
    false
  }

  def query_cluster: Boolean = {
    val spark = SparkSession.builder
      .master("spark://lagrange:7077")
      .appName("Spark SQL Example")
//      .config("spark.submit.deployMode", "cluster")
      .config("spark.driver.userClassPathFirst", true)
      .getOrCreate()

//    Class.forName("org.postgresql.Driver")
    val sc = spark.sparkContext
    println(sc.getConf.toDebugString)
    sc.addJar("file:/home/cdb/projects/mondriamo/lib/jars/org.postgresql.postgresql-9.3-1103-jdbc41.jar")
    val jdbcDF = spark.read
      .format("jdbc")
      .options(Map("url" -> "jdbc:postgresql://localhost:5432/article_performance_666570051",
                   "dbtable" -> "public.article_cost_facts2"))
      .load()
    jdbcDF.createOrReplaceTempView("articles")

    val jdbc2DF = spark.read
      .format("jdbc")
      .options(
        Map("url" -> "jdbc:postgresql://localhost:5432/article_performance_666570051", "dbtable" -> "public.dates"))
      .load()

    jdbc2DF.createOrReplaceTempView("dates")

    val result = spark.sql(
      "SELECT d.year, d.day_of_month, a.article_id FROM articles as a JOIN dates as d on a.date_key = d.key where a.article_id > 96000 limit 10")
    result.show()

    val cat = spark.catalog
    cat.listTables("default").show()
    cat.listDatabases().show()
    spark.stop()
    false
  }
}
