package template.spark


import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Row}

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql

trait InitSpark {
  val spark: SparkSession = SparkSession.builder()
                            .appName("Spark example")
                            .master("local[*]")
                            .config("option", "some-value")
                            .getOrCreate()
  println("st1")

  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext
  def reader = spark.read
               .option("header",true)
               .option("inferSchema", true)
               .option("mode", "DROPMALFORMED")
  println("st2")

  def readerWithoutHeader = spark.read
                            .option("header",true)
                            .option("inferSchema", true)
                            .option("mode", "DROPMALFORMED")

  println("st3")

  private def init = {
    print("IN INIT method")
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger.setLevel(Level.ERROR)
  }
init
  println("st4")

  def close = {
    println("executed")
    spark.close()
  }



  def assertDataFrameEquals(a: DataFrame, b: DataFrame, isRelaxed: Boolean): Boolean = {

    try {

      a.rdd.cache
      b.rdd.cache

      // 1. Check the equality of two schemas
      if (!a.schema.toString().equalsIgnoreCase(b.schema.toString)) {
        println("Schema is not equal")
        //return false

      }

      println("Souce Count:"+a.count())
      println("Target Count"+b.count())
      // 2. Check the number of rows in two dfs
      if (a.count() != b.count()) {

        println("Number of rowa are not  equal")

        //return false
      }
      else
        println("Number of rowa are   equal")


      // 3. Check there is no unequal rows
      val aColumns: Array[String] = a.columns
      val bColumns: Array[String] = b.columns

      // To correctly handles cases where the DataFrames may have columns in different orders
      //scala.util.Sorting.quickSort(aColumns)
      //scala.util.Sorting.quickSort(bColumns)
      val aSeq: Seq[Column] = aColumns.map(col(_))
      val bSeq: Seq[Column] = bColumns.map(col(_))

      var a_prime: DataFrame = null
      var b_prime: DataFrame = null

      //println("isRelaxed in  "+isRelaxed)

      if (isRelaxed) {
      //  println("isRelaxed in if "+isRelaxed)
        a_prime = a
                  a_prime.show()
        b_prime = b
                 b_prime.show()
      }
      else {
        // To correctly handles cases where the DataFrames may have duplicate rows and/or rows in different orders
       // println("isRelaxed in else "+isRelaxed)

        a_prime = a.sort(aSeq: _*).groupBy(aSeq: _*).count()
           a_prime.show()
        b_prime = b.sort(bSeq: _*).groupBy(bSeq: _*).count()
          b_prime.show()
      }

      val c1: Long = a_prime.except(b_prime).count()
      val c2: Long = b_prime.except(a_prime).count()

      if (c1 != c2 || c1 != 0 || c2 != 0) {
        return false
      }

    } finally {
      a.rdd.unpersist()
      b.rdd.unpersist()
    }

    true
  }





}
