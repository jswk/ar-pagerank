/**
 * Created by malawski on 10/25/14.
 */

  import org.apache.spark.SparkContext._
  import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

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
    if (args.length < 3) {
      System.err.println("Usage: SparkPageRank <file> <nodes> <lvl of parallelism>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("Connected Components")
    sparkConf.set("spark.default.parallelism", args(2))
    val ctx = new SparkContext(sparkConf)

    val nodeCount = args(1).toInt
    println("Input file: "+args(0))
    println("Nodes: "+nodeCount)

    val startMs = System.currentTimeMillis()

    val lines = ctx.textFile(args(0))
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (Integer.parseInt(parts(0)), Integer.parseInt(parts(1)))
    }.flatMap {
      case (u, v) =>
        List((u, v), (v, u))
    }.partitionBy(new HashPartitioner(nodeCount))
      .distinct()
      .groupByKey()
      .cache()
    var ranks = links.keys.map(u => (u, u))

    var ok = true
    var iterations = 0
    while (ok) {
      val itStart = System.currentTimeMillis()

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

      val itEnd = System.currentTimeMillis()

      println("Iteration: "+iterations+" Changes: "+changes.value+ " Leap: "+(itEnd-itStart))
    }

    val output = ranks.collect()

    val endMs = System.currentTimeMillis()

    println("iterations: "+iterations)
    println("time: "+(endMs-startMs))

    // output.foreach(tup => println(tup._1 + " " + tup._2))

    ctx.stop()
  }
}


