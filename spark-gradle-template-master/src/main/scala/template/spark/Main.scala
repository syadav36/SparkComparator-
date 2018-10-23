package template.spark

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.functions._
import org.apache.spark.sql
import org.apache.spark.SparkConf

final case class Person(id: String,firstName: String)

case class employees(firstName: String, id: String)
case class departments(firstName: String, id1: String)



object Main extends InitSpark {


  def main(args: Array[String]) = {
    import spark.implicits._


    val version = spark.version
    println("SPARK VERSION = " + version)



  //  println("Reading from csv file: people-example.csv")
   // val persons = reader.csv("people-example.csv").as[Person].toDF("id","firstName","lastName","country","age"
   // )






    val df=spark.read
      .format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true) // <-- HERE
      .csv("people-example.csv");
//l


    df.printSchema()


    print("reading source")

    val df1=spark.read
      .format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true) // <-- HERE
      .csv("idread.csv")





    //r
    df.printSchema()
    print("reading target")
    df1.show




    //val df = Seq(("1", "2", "3"), ("2", "3", "4")).toDF("col1_term1", "col2_term2", "col3_term3")



    // df_res_m
     // df.write.csv("/Users/shailesh/Documents/abc1.csv")

    df1.registerTempTable("TEMP_TABL")

var sql_input="SELECT COUNT(id) AS ID,sum(id) ,firstName FROM TEMP_TABL GROUP BY firstName order by 3"

var dff=sqlContext.sql(sql_input)

dff.show()
    println("started execution")
    var  isRelaxed=false

    val final_check =assertDataFrameEquals(df,df1,isRelaxed)
    println(final_check)


//CONNECTING TO SQL SEVER***************************************

    /*
    val jdbcHostname = "localhost"
    val jdbcPort = 1433
    val jdbcDatabase = "test"
   val jdbcUsername="sa"
   val jdbcPassword="Shailesh1"
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")


    // Create the JDBC URL without passing in the user and password parameters.
   // val jdbcUrl = "jdbc:microsoft:sqlserver://localhost:1433/test"
    val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
    // Create a Properties() object to hold the parameters.
    import java.util.Properties
    val connectionProperties = new Properties()

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")

    val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    connectionProperties.setProperty("Driver", driverClass)

    val employees_table = spark.read.jdbc(jdbcUrl, "emp", connectionProperties)


    employees_table.printSchema()
    employees_table.select("*").show()

*/
//********************************sql SEVER READ END

    //**********************************MYSQL Connect**************

    /*
    Class.forName("com.mysql.jdbc.Driver") // Databricks Runtime 3.3 and below

    val jdbcHostname = "localhost"
    val jdbcPort = 3306
    val jdbcDatabase = "test"
    val jdbcUsername="root"
    val jdbcPassword="root"
    // Create the JDBC URL without passing in the user and password parameters.
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

    // Create a Properties() object to hold the parameters.
    import java.util.Properties
    val connectionProperties = new Properties()

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")


    val employees_table = spark.read.jdbc(jdbcUrl, "emp", connectionProperties)


    employees_table.printSchema()
    employees_table.select("*").show()


    import java.sql.DriverManager
    val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
    connection.isClosed()
    */

//***********************************************mysql read end

    var columnsdf1=Seq((Array(1,1))).toDF("id").columns
    var  columnsdf2 =Seq((Array(1,1))
    ).toDF("id1").columns

    var joinExprs1 = columnsdf1
      .zip(columnsdf2)
      .map{case (c1, c2) => df1(c1) === df(c2)
      }
      .reduce(_ && _)


    def mapDiffs(name: String) = when($"l.$name" === $"r.$name", null)

      .otherwise(array($"l.$name", $"r.$name"))
      .as(name)

    var cols = df.columns.filter(_ != "id1").toList
    var df_res = df.alias("l").join(df1.alias("r"),joinExprs1,"full_outer")
      //.select($"id" :: cols.map(mapDiffs): _*)

    //selecting all fields with primary key combo
    val df_res1=df_res.select("r.*","l.id1").alias("src")
    val df_res2=df_res.select("l.*","r.id").alias("tgt")


    df_res1.show

println("printing")
    //val df_res= df1.alias("l")
     // .join(df.alias("r"),
     //   $"$source_col" === $"$target_col",
        //"full_outer").select($"id" :: cols.map(mapDiffs): _*)


    //df_res.show()

    val source_col="id"

    val target_col="id1"
   // val result = df.as("l")
     // .join(df1.as("r"), "id")
   //   .select($"id" :: cols.map(mapDiffs): _*)

 //   result.show()


    val df_res_left= df_res1
   .filter($"$target_col".isNull ).select("src.*")
    print("left show")


    df_res_left.select("src.*").coalesce(1).write.
     format("com.databricks.spark.csv").option("header", "true").
     save("/Users/shailesh/Documents/" + "Source Not Matching with target")


    val df_res_right= df_res2
      .filter($"$source_col".isNull).select("tgt.*")
    print("right show")



   df_res_right.dropDuplicates().coalesce(1).write.
      format("com.databricks.spark.csv").option("header", "true").
     save("/Users/shailesh/Documents/" + "Target Value  not matching with source")


    val df_res_match=df_res
          .filter($"$source_col" === $"$target_col" ).dropDuplicates()

    .select($"id" :: $"id1" ::cols.map(mapDiffs): _*)


    //df_res_match.foreach { row =>
    //  println(row.mkString(","))
   // }




    val stringify = udf((vs: Seq[String]) => vs match {
      case null => null
      case _    => s"""[${vs.mkString(",")}]"""
    })


  val column_names: Array[String] = df_res_match.drop("id").drop("id1")
      .select("*").
      columns





    val  column_frame = df_res_match.withColumn("age", 'age cast "string").
      withColumn("lastName", 'lastName cast "string").
      withColumn("country", 'country cast "string").
      withColumn("firstName", 'firstName cast "string").
      drop("id1").select("id","age","lastName","firstName","country").
      coalesce(1).write.
      format("com.databricks.spark.csv").option("header", "true").
      save("/Users/shailesh/Documents/" + "Source_Target_mismatch")

    //drop().select (concat_ws(",", selection: _*)
    //df_res_match.select(concat($"age.cast("array<string>",$"country")).show()


/*

        for(i <- 0 until column_names.length){
          var str=column_names(i)
          println("i'th element is: " + column_names(i))
          column_frame.withColumn(column_names(i), stringify($"$str")).select(str).
            coalesce(1).write.
          format("com.databricks.spark.csv").option("header", "true").
            save("/Users/shailesh/Documents/"+ str + "Missing in Target")


        }


*/
      // df_res_match.withColumn("lastName", stringify($"lastName")).select("lastName").show
      // coalesce(1).write.
      //format("com.databricks.spark.csv").option("header", "true").save("/Users/shailesh/Documents/zz1.csv" + "Missing in Target")

      // df_res_match.coalesce(1).write.
      // format("com.databricks.spark.csv").option("header", "true").save("/Users/shailesh/Documents" + "Missing in Target")


      //---String Conversion


      close
  }
}

