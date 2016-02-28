
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.xml.{XML,NodeSeq}

object SparkPageRank {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("PageRank")
    val iters = if (args.length > 2) args(2).toInt else 10
    val ctx = new SparkContext(sparkConf)
    val input = ctx.textFile(args(0))
    //parse input file
    var vertices = input.map(line => {
      val fields = line.split("\t")
      val (title, body) = (fields(1), fields(3).replace("\\n", "\n"))
      val links =
        if (body == "\\N")
          NodeSeq.Empty
        else
          try {
            XML.loadString(body) \\ "link" \ "target"
          } catch {
            case e: org.xml.sax.SAXParseException =>
              System.err.print()
            NodeSeq.Empty
          }
      val outEdges = links.map(link => new String(link.text)).toArray
      val id = new String(title)
      (id,outEdges)
    })

    //var tmp = vertices.flatMap(a => {a._2.map {t => (a._1,t)}})

    //val links = tmp.distinct().groupByKey().cache()
    
    vertices = vertices.distinct.cache()

    var ranks = vertices.mapValues(v => 1.0)
    for (i <- 1 to iters) {
      println(s"****** Starting iteration : $i ******")
      val contribs = vertices.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val result = ranks.top(100) {
      Ordering.by((entry: (String, Double)) => entry._2)
    }

    ctx.makeRDD(result).saveAsTextFile(args(1))
    ctx.stop()
  }
}
