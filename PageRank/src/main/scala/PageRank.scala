
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import scala.xml.{XML,NodeSeq}

//sample command 
//./bin/spark-submit --class PurePageRank [pathToJar]/PageRank.jar [inputFile] [outputFIle] [itr]
object PurePageRank extends Logging{
  def main(args: Array[String]) {
    //check for valid no.of args 
    if (args.length < 1) {
      //exit if less than 1
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }
    //start time
    val start = System.currentTimeMillis
    
    val sparkConfig = new SparkConf().setAppName("PageRank")
    val iters = if (args.length > 2) args(2).toInt else 10
    val context = new SparkContext(sparkConfig)
    //read input file
    val wikiData = context.textFile(args(0))
    logWarning("Starting vertex generation");
    //start parsing input file
    var vertices = wikiData.map(line => {
      //split line by tab delimiter
      val fields = line.split("\t")
      //pick second and third field of the line 
      val (title, body) = (fields(1), fields(3).replace("\\n", "\n"))
      //extract external links from 
      val links =
        if (body == "\\N")
          NodeSeq.Empty
        else
          try {
            //extract link -> target tag
            XML.loadString(body) \\ "link" \ "target"
          } catch {
            case e: org.xml.sax.SAXParseException =>
              System.err.print("Error while reading xml structure")
            NodeSeq.Empty
          }
      //text data from links-> target tags
      val outEdges = links.map(link => new String(link.text)).toArray
      //title
      val srcTitle = new String(title)
      (srcTitle,outEdges)
    }).cache()

    logWarning("Completed vertex generation");
    //assign initial ranks as 1
    var ranks = vertices.mapValues(v => 1.0)

    logWarning("Starting PageRank iterations");
    for (i <- 1 to iters) {
      //println(s"****** Starting iteration : $i ******")
      //calculate pagerank for each iteration
      val contribs = vertices.join(ranks).values.flatMap{ 
        case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    logWarning("Ended PageRank iterations");
    val result = ranks.top(10000) {
      Ordering.by((entry: (String, Double)) => entry._2)
    }
    //output sigle outputFile 
    context.makeRDD(result).coalesce(1,true).saveAsTextFile(args(1))
    logWarning("Graphx pagerank took time: " + (System.currentTimeMillis - start)/1000.0+" sec.")
    context.stop()
  }
}
