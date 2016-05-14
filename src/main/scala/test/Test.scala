package test

import com.databricks.spark.sql.perf.tpch.Tables
import com.databricks.spark.sql.perf.tpch.TPCH
import java.nio.file.{Paths, Files}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import java.util.Calendar
import com.databricks.spark.sql.perf.Benchmark
import playground._
import java.io._

object Test {
  val usage = """
  Usage: dspark [iterations num] [sf num] [queries filter*] [format [ parquet | ... ]] [cached]
  """

  def main(args: Array[String]): Unit = {

    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    val configuration = new SparkConf()
        .setAppName("Benchmark")
        .setMaster("local[*]")

    val tpch = new TPCH(new SQLContext(new SparkContext(configuration)))

    tpch.sqlContext.sparkContext.getConf.getAll foreach {
      case (a, b) => println(s"$a: $b")
    }

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "iterations" :: value :: tail =>
          nextOption(map + ('iterations -> value.toInt), tail)
        case "sf" :: value :: tail =>
          nextOption(map + ('sf -> value.toInt), tail)
        case "appname" :: value :: tail =>
          nextOption(map + ('appname -> value), tail)
        case "format" :: value :: tail =>
          nextOption(map + ('format -> value), tail)
        case "cached" :: tail =>
          nextOption(map + ('cached -> true), tail)
        case "queries" :: value :: tail =>
          nextOption(map + ('queries -> value), tail)
        case "delite" :: tail =>
          nextOption(map + ('delite -> true), tail)
        case option :: _ => println("Unknown option "+option)
          System.exit(0)
          Map()
      }
    }

    val defaultoption = Map('iterations -> 5.asInstanceOf[Any], 'sf -> 1.asInstanceOf[Any] ,'appname -> "Spark-Perf".asInstanceOf[Any], 'format -> "com.databricks.spark.csv".asInstanceOf[Any], 'cached -> false.asInstanceOf[Any], 'queries -> "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22".asInstanceOf[Any], 'delite -> false)
    val options = nextOption(defaultoption.asInstanceOf[OptionMap], arglist)
    println("Option used:")

    options foreach {case (a, b) => println(" " + a + "\n\t: " + b)}

    val sf = (options apply 'sf).asInstanceOf[Int]

    val folder = sys.env("DELITE_PLAY") + f"/data/SF$sf%d"
    println(folder)
    if (!Files.exists(Paths.get(folder))) {
      println(s"[ERROR] $folder folder doesn't exists")
      System.exit(1)
    }

    val today = Calendar.getInstance().getTime()
    val date = today.toString.substring(4, 19).replace(" ", "_").replace(":","_")
    val format = (options apply 'format).toString
    val cached = (options apply 'cached).asInstanceOf[Boolean]

    val outputfolder = f"spark_sf$sf%d_$date%s_$format%s_$cached%b"
    println(outputfolder)

    val tables = new Tables(tpch.sqlContext, "", 1)
    tables.createTemporaryTables(folder, format)

    val delite = (options apply 'delite).asInstanceOf[Boolean]

    if (delite) {
      // System.setProperty("delite.pinThreads", "true")
      (1 to (options apply 'iterations).asInstanceOf[Int]).foreach( _ =>
        (options apply 'queries).asInstanceOf[String].split(",").map({
            (n:String) => tpch.tpch_opt_string.filter({ case (p, _) => !p.endsWith("orig")})(n.toInt-1)
          }).foreach({
            case (name, sql: String) =>
              println(sql)
              val pw = new FileWriter("result.csv", true)
              pw.write("\n" + name + ",")
              pw.flush
              pw.close

              Run.runDelite(tpch.sqlContext.sql(sql).queryExecution.optimizedPlan, cached)
              Thread sleep 1000
          })
      )
    } else {


        val queries = (options apply 'queries).asInstanceOf[String].split(",") map {
          n => tpch.tpch(n.toInt-1)
        }

        val experiment = if (cached)
        tpch.runExperiment(queries, iterations = (options apply 'iterations).asInstanceOf[Int], variations = Seq(tpch.preload), outputfolder = outputfolder)
      else
        tpch.runExperiment(queries, iterations = (options apply 'iterations).asInstanceOf[Int], outputfolder = outputfolder)

      experiment.waitForFinish(7 * 24 * 3600)
      }
  }
}

