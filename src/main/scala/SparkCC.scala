/**
 * Created by malawski on 10/25/14.
 */

  import org.apache.spark.SparkContext._
  import org.apache.spark.{SparkConf, SparkContext}

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 */
object SparkCC {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("Connected Components")
    val iters = if (args.length > 0) args(1).toInt else 10
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile(args(0), 1)
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (Integer.parseInt(parts(0)), Integer.parseInt(parts(1)))
    }.flatMap {
      case (u, v) =>
        List((u, v), (v, u))
    }.distinct().groupByKey().cache()
    var ranks = links.keys.map(u => (u, u))

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap {
        case (urls, rank) =>
          urls.map(url => (url, rank))
      }
      val minRanks = contribs.reduceByKey(Math.min)
      ranks = minRanks.join(ranks).map {
        case (url, (ownRank, neighborRank)) =>
          (url, Math.min(ownRank, neighborRank))
      }
    }

    val output = ranks.collect()
    output.foreach(tup => println(tup._1 + " " + tup._2))

    ctx.stop()
  }
}


