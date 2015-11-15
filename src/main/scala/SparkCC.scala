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
    val ctx = new SparkContext(sparkConf)

    val startMs = System.currentTimeMillis()

    val lines = ctx.textFile(args(0), 1)
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (Integer.parseInt(parts(0)), Integer.parseInt(parts(1)))
    }.flatMap {
      case (u, v) =>
        List((u, v), (v, u))
    }.distinct().groupByKey().cache()
    var ranks = links.keys.map(u => (u, u))

    var ok = true
    var iterations = 0
    while (ok) {
      val changes = ctx.accumulator(0)
      val contribs = links.join(ranks).values.flatMap {
        case (urls, rank) =>
          urls.map(url => (url, rank))
      }
      val minRanks = contribs.reduceByKey(Math.min)
      ranks = ranks.join(minRanks).map {
        case (url, (ownRank, neighborRank)) =>
          if (ownRank > neighborRank) {
            changes += 1
            (url, neighborRank)
          } else {
            (url, ownRank)
          }
      }
      ranks.cache()
      ranks.count()

      iterations += 1
      ok = changes.value > 0
    }

    val output = ranks.collect()

    val endMs = System.currentTimeMillis()

    println("iterations: "+iterations)
    println("time: "+(endMs-startMs))

    output.foreach(tup => println(tup._1 + " " + tup._2))

    ctx.stop()
  }
}


