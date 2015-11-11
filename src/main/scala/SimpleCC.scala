
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object SimpleCC {
  def main(args: Array[String]) {
    val ITERATIONS = 10

    val conf = new SparkConf().setAppName("Simple Connected Components")
    val sc = new SparkContext(conf)


    //  Prepare data
    val linksData = Array((1, 3), (2, 1), (3,1), (3,4), (4,1), (4,2), (5,6))

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

    for (i <- 1 to ITERATIONS) {
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

    // Return an array that contains all of the elements in this RDD.
    val output = ranks.collect()

    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

    sc.stop()
  }

  def min(a:Int, b:Int):Int = {
    Math.min(a, b)
  }
}
