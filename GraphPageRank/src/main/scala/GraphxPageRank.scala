import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.xml.{XML,NodeSeq}
import org.apache.spark.graphx.lib._

//sample command 
//./bin/spark-submit --class GraphxPageRank [pathToJar]/PageRank.jar [inputFile] [outputFIle] [itr]
object GraphxPageRank extends Logging{
	def main(args: Array[String]) {
		//check for arguement count
		if(args.length < 2) {
			//if less than return error statement
			println("Invalid arguments count")		
			return
		}
		val start = System.currentTimeMillis

		//reading the path locations
		var inPath : String = args(0)			
		var outPath: String = args(1) 
		val itrs = if(args.length > 2) args(2).toInt else 10
		val conf = new SparkConf().setAppName("GraphPageRank")
		val sc = new SparkContext(conf)
		
		val input: RDD[String] = sc.textFile(inPath)
		logWarning("Starting vertex generation");
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
	    }).cache
	    
	    logWarning("Completed vertex generation");
		//mapping each src vertex with dest vertex
    	var tmp = vertices.flatMap(a => {a._2.map {t => (a._1,t)}})

    	// Hash function to assign an Id to each vertex
		def myHashCode(s: String): Long = {
			// prime
		    var h: Long = 1125899906842597L  
		    val len: Int = s.length
		    var i = 0
		    while (i < len) {
		      h = 31*h + s.charAt(i)
		      i += 1
		    }
		    h
		}

		//create graphvertices from vertices
    	val gVertices = vertices.map( a => (myHashCode(a._1),a._1)).cache
    	val edges: RDD[Edge[Double]] = tmp.map { a =>
    		val srcVid = myHashCode(a._1)
    		val dstVid = myHashCode(a._2)
    		Edge(srcVid,dstVid,1.0)
    	}

    	logWarning("Started graph generation");
    	val graph = Graph(gVertices, edges, "").subgraph(vpred = {(v, d) => d.nonEmpty}).cache
    	
    	logWarning("Starting PageRank iterations");
		val prGraph = graph.staticPageRank(itrs,.15).cache
		val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
		  (v, title, rank) => (rank.getOrElse(0.0), title)
		}
		//format output by fetching the top 100 and by the order of rank.
		val output = titleAndPrGraph.vertices.top(100) {
		  Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
		}.map { t => (t._2._2,t._2._1)}

		sc.makeRDD(output).coalesce(1,true).saveAsTextFile(outPath)

		logWarning("Graphx pagerank took time: " + (System.currentTimeMillis - start)/1000.0+" sec.")
	}
}