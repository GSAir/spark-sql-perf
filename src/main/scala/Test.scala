import com.databricks.spark.sql.perf.tpch.Tables
import com.databricks.spark.sql.perf.tpch.TPCH
import java.nio.file.{Paths, Files}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import java.util.Calendar

object Test {
  val usage = """
  Usage: dspark [iterations num] [sf num] [queries filter*]
  """

  def main(args: Array[String]): Unit = {

    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]
    val tpch = new TPCH()

    val ooo = tpch.sqlContext.sparkContext.getConf.getAll.length
    println(s"Length: $ooo")
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
        // case "queries" :: value :: tail =>
        //   val filter = value.split(",")
        //   nextOption(map + ('queries -> tpch.tpch.filter { case Query(name, _, _, _) => filter.contains(name) }), tail)
        case option :: _ => println("Unknown option "+option)
          System.exit(0)
          Map()
      }
    }

    val defaultoption = Map('iterations -> 5.asInstanceOf[Any], 'sf -> 1.asInstanceOf[Any] ,'appname -> "Spark-Perf".asInstanceOf[Any] )
    val options = nextOption(defaultoption.asInstanceOf[OptionMap], arglist)
    println("Option used:")

    options foreach {case (a, b) => println(" " + a + "\n\t: " + b)}

    val folder = sys.env("DELITE_PLAY") + "/data/SF" + (options apply 'sf).toString
    println(folder)
    if (!Files.exists(Paths.get(folder))) {
      println(s"[ERROR] $folder folder doesn't exists")
      System.exit(1)
    }

    val today = Calendar.getInstance().getTime()
    val outputfolder = "spark_sf" + (options apply 'sf).toString + "_" + today.toString.replace(" ", "_")
    println(outputfolder)

    val tables = new Tables(tpch.sqlContext, "", 1)
    tables.createTemporaryTables(folder, "com.databricks.spark.csv")

    val experiment = tpch.runExperiment(tpch.tpch, iterations = (options getOrElse ('iterations, 3)).asInstanceOf[Int], outputfolder = outputfolder)

    experiment.waitForFinish(7 * 24 * 3600)
  }
}

