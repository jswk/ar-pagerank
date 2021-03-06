
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object SimpleCC {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Connected Components")
    val sc = new SparkContext(conf)

    val startMs = System.currentTimeMillis()

    //  Prepare data
    val linksData = Array((1, 2), (2, 3), (3, 4), (5,6))

    //  RDD of (url, url) pairs
    //  RDD[(String, String)]
    val linksRDD = sc.parallelize(linksData).flatMap {
      case (u, v) =>
        List((u, v), (v, u))
    }

    //  RDD of (url, neighbors) pairs
    //  RDD[(String, Iterable[String])]
    val links = linksRDD.distinct().groupByKey().cache()

    // RDD of (url, rank) pairs
    // RDD[(String, Double)]
    // Pass each value in the key-value pair RDD through a map function without changing the keys; this also retains the original RDD's partitioning.
    var ranks = links.keys.map(u => (u, u))

    var ok = true
    var iterations = 0
    while (ok) {
      val changes = sc.accumulator(0)
      val contribs = links.join(ranks).values.flatMap {
        case (urls, rank) =>
          urls.map(url => (url, rank))
      }
      val minRanks = contribs.reduceByKey(Math.min)

      ranks = ranks.join(minRanks).map {
        case (url, (ownRank, neighborRank)) => {
          if (ownRank > neighborRank) {
            changes += 1
            (url, neighborRank)
          } else {
            (url, ownRank)
          }
        }
      }
      ranks.cache()
      ranks.count()

      iterations += 1
      ok = changes.value > 0
    }

    // Return an array that contains all of the elements in this RDD.
    val output = ranks.collect()

    val endMs = System.currentTimeMillis()

    println("iterations: "+iterations)
    println("time: "+(endMs-startMs))

    output.foreach(tup => println(tup._1 + " " + tup._2))

    sc.stop()
  }
}
