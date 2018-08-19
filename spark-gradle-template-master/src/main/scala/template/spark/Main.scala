package template.spark


import org.apache.spark.sql.functions._
import  org.apache.spark.sql
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
      .csv("people-example.csv")

    df.printSchema()
    print("reading source")
    df.show

    val df1=spark.read
      .format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true) // <-- HERE
      .csv("idread.csv")

    df.printSchema()
    print("reading target")
    df1.show


    df1.registerTempTable("TEMP_TABL")

var sql_input="SELECT COUNT(id) AS ID,sum(id) ,firstName FROM TEMP_TABL GROUP BY firstName order by 3"

var dff=sqlContext.sql(sql_input)

dff.show()
    println("started execution")
    var  isRelaxed=false

    val final_check =assertDataFrameEquals(df,df1,isRelaxed)
    println(final_check)



    var columnsdf1=Seq((Array(1,1),"")).toDF("id","firstName").columns
    var  columnsdf2 =Seq((Array(1,1),"")).toDF("id1","firstName").columns

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
      .select($"id" :: cols.map(mapDiffs): _*)

    df_res.show()




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


    val df_res_left= df_res
   .filter($"$source_col".isNull )
    print("left show")
    val df_res_left_show=df_res_left.show
    val df_res_right= df_res
      .filter($"$target_col".isNull)
    print("right show")

    val df_res_right_show=df_res_right.show

        val df_res_match=df_res
          .filter($"$source_col" === $"$target_col" ).dropDuplicates()






    df_res_match.show()








    close
  }
}

