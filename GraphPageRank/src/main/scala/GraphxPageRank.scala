import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.xml.{XML,NodeSeq}

//sample command 
//./bin/spark-submit --class WordCount --master local[4] [pathToJar]/PageRank.jar [inputFile] [outputFIle]
object PageRank {
	def main(args: Array[String]) {
		//check for arguement count
		if(args.length < 2) {
			//if less than return error statement
			println("Invalid arguments count")		
			return
		}
		//reading the path locations
		var inPath : String = args(0)			
		var outPath: String = args(1) 
		val itrs = if(args.length > 2) args(2).toInt else 10
		val conf = new SparkConf().setAppName("GraphPageRank")
		val sc = new SparkContext(conf)
		
		val input: RDD[String] = sc.textFile(inPath)
		
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
	    vertices = vertices.distinct.cache

		//mapping each src vertex with dest vertex
    	var tmp = vertices.flatMap(a => {a._2.map {t => (a._1,t)}})
    	
    	// tmp = tmp.distinct()
    	//val links = tmp.distinct().groupByKey().cache()

    	// Hash function to assign an Id to each article
		def pageHash(title: String): VertexId = {
		  title.replace(" ","").toLowerCase.hashCode.toLong
		}

		def myHashCode(s: String): Long = {
		    var h: Long = 1125899906842597L  // prime
		    // var h = 29
		    val len: Int = s.length
		    var i = 0
		    while (i < len) {
		      h = 31*h + s.charAt(i)
		      i += 1
		    }
		    h
		}

    	val gVertices = vertices.map( a => (myHashCode(a._1),a._1)).cache
    	val edges: RDD[Edge[Double]] = tmp.map { a =>
    		val srcVid = myHashCode(a._1)
    		val dstVid = myHashCode(a._2)
    		Edge(srcVid,dstVid,1.0)
    	}

    	val graph = Graph(gVertices, edges, "").subgraph(vpred = {(v, d) => d.nonEmpty}).cache
		val prGraph = graph.staticPageRank(itrs).cache
		val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
		  (v, title, rank) => (rank.getOrElse(0.0), title)
		}

		val output = titleAndPrGraph.vertices.top(100) {
		  Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
		}.map { t => (t._2._2,t._2._1)}//.foreach(t => println(t._2._2 + ": " + t._2._1))


		sc.makeRDD(output).saveAsTextFile(outPath)




		// /************ graph algo starts here ************/
		// case class Article(val title: String, val body: String)
		// // Parse the articles
		// val articles = wiki.map(_.split('\t')).
		//   // two filters on article format
		//   filter(line => (line.length > 1 && !(line(3) contains "REDIRECT"))).
		//   // store the results in an object for easier access
		//   map(line => new Article(line(1).trim, line(3).trim)).cache
		
		
		// The vertices with id and article title:
		// val vertices = articles.map(a => (pageHash(a.title), a.title)).cache
		// val pattern = "\\[\\[.+?\\]\\]".r
		// val edges: RDD[Edge[Double]] = articles.flatMap { a =>
		//   val srcVid = pageHash(a.title)
		//   pattern.findAllIn(a.body).map { link =>
		//     val dstVid = pageHash(link.replace("[[", "").replace("]]", ""))
		//     Edge(srcVid, dstVid, 1.0)
		//   }
		// }

		// val graph = Graph(vertices, edges, "").subgraph(vpred = {(v, d) => d.nonEmpty}).cache
		// val prGraph = graph.staticPageRank(itrs).cache
		// val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
		//   (v, title, rank) => (rank.getOrElse(0.0), title)
		// }

		// val test = titleAndPrGraph.vertices.top(100) {
		//   Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
		// }
		// //.foreach(t => println(t._2._2 + ": " + t._2._1))


		// sc.makeRDD(test).saveAsTextFile(outPath)
	}
}
