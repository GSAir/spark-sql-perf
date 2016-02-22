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
  Usage: dspark [iterations num] [sf num] [queries filter*] [format [ parquet | ... ]] [cached]
  """

  def main(args: Array[String]): Unit = {

    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]
    val tpch = new TPCH()

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
        // case "queries" :: value :: tail =>
        //   val filter = value.split(",")
        //   nextOption(map + ('queries -> tpch.tpch.filter { case Query(name, _, _, _) => filter.contains(name) }), tail)
        case option :: _ => println("Unknown option "+option)
          System.exit(0)
          Map()
      }
    }

    val defaultoption = Map('iterations -> 5.asInstanceOf[Any], 'sf -> 1.asInstanceOf[Any] ,'appname -> "Spark-Perf".asInstanceOf[Any], 'format -> "csv".asInstanceOf[Any], 'cached -> false.asInstanceOf[Any] )
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
    val date = today.toString.replace(" ", "_")
    val format = (options apply 'format).toString
    val cached = (options apply 'cached).asInstanceOf[Boolean]

    val outputfolder = f"spark_sf$sf%d_$date%s_$format%s_$cached%b"
    println(outputfolder)

    val tables = new Tables(tpch.sqlContext, "", 1)
    tables.createTemporaryTables(folder, format, cached = cached)

    val experiment = tpch.runExperiment(tpch.tpch, iterations = (options apply 'iterations).asInstanceOf[Int], outputfolder = outputfolder)

    experiment.waitForFinish(7 * 24 * 3600)
  }
}

