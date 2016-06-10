/* WordCount.scala */
import org.apache.spark._
import org.apache.hadoop.fs._
import java.util.regex._
// import java.util._
import java.lang.Double
import java.lang.Math
import scala.collection.mutable.ListBuffer
import org.apache.spark.mllib.linalg._

object WordCount {
    def main(args: Array[String]) {
        if (args.length < 1) {
          println("Please input size !")
          System.exit(1)
        }

        val files = "/shared/HW2/sample-in/input-" + args(0)
        val outputPath = "HW2_Spark_" + args(0)
        val conf = new SparkConf().setAppName("PageRank")
        val sc = new SparkContext(conf)
        
        // Cleanup output dir
        val hadoopConf = sc.hadoopConfiguration
        val hdfs = FileSystem.get(hadoopConf)
        try { hdfs.delete(new Path(outputPath), true) } catch { case _ : Throwable => { } }
        
        val lines = sc.textFile(files)
        val masterGraph = lines.flatMap(line =>{
          val titleReg = "<title>(.+?)</title>"
          val titleSplitReg = "<title>|</title>"
          val m = Pattern.compile(titleReg).matcher(line)
          val isFind = m.find()
          val titleStr = m.group().split(titleSplitReg)
        
          titleStr.filter(_.length() != 0).map(word => {
            val title = word.replaceAll("&quot;", "\"")
                            .replaceAll("&gt;", ">")
                            .replaceAll("&lt;", "<")
                            .replaceAll("&amp;","&")
                            .replaceAll("&apos;", "'")
            
            val nextNodes = new Page(List[String]())
            nextNodes.setMaster()
            (title, nextNodes)
          })
        }).cache

        val slaveGraph = lines.flatMap(line =>{
          val titleReg = "<title>(.+?)</title>"
          val titleSplitReg = "<title>|</title>"
          val m = Pattern.compile(titleReg).matcher(line)
          val mNode = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])").matcher(line)
          val isFind = m.find()
          val titleStr = m.group().split(titleSplitReg)
        
          titleStr.filter(_.length() != 0).flatMap(word => {
            val title = word.replaceAll("&quot;", "\"")
                            .replaceAll("&gt;", ">")
                            .replaceAll("&lt;", "<")
                            .replaceAll("&amp;","&")
                            .replaceAll("&apos;", "'")
            var allMatches: List[String] = List[String]() 
            while (mNode.find()) {
              val patterns = mNode.group().split("[\\[\\]\\|#]+")
              if (patterns.length > 1) {
                val nextNode = patterns(1).replaceAll("&quot;", "\"")
                                  .replaceAll("&gt;", ">")
                                  .replaceAll("&lt;", "<")
                                  .replaceAll("&amp;","&")
                                  .replaceAll("&apos;", "'").capitalize
                allMatches = nextNode.capitalize :: allMatches
              }
            }

            allMatches.map(nextNode => {
              val preNode = List(title)
              (nextNode, new Page(preNode))
            })
          })
        })
        
        val originGraph = slaveGraph.union(masterGraph)
                          .reduceByKey(_++_)
                          .filter(node => node._2.getRole() == 0)
                          .flatMap(node => {
                            node._2.getEdges().map(preNode => {
                              (preNode, new Page(List[String](node._1)))
                            })
                          })
                          .union(masterGraph)
                          .reduceByKey(_++_)
        masterGraph.unpersist()
        //originGraph.saveAsTextFile(outputPath)


        val N = originGraph.count
        var graph = originGraph.map(node => {
          node._2.setPageRank(Double.valueOf(1) / Double.valueOf(N))
          (node._1, node._2)
        }).persist()

        val alpha = new Double(0.85)
        var error = new Double(1)
        val one = new Double(1)
        var numOfIteration = 0
        while (error.compareTo(0.001) >= 0) {
          numOfIteration = numOfIteration + 1
          val zeroDegree = graph.filter(node => {
            node._2.getNumOfEdges() == 0
          }).map(node => {
            node._2.getPageRank()
          }).reduce(_+_) * alpha / Double.valueOf(N) + (one - alpha) * (one / Double.valueOf(N))
         
          println("[ZERODEGREE] " + zeroDegree.toString())

          val sumGraph = graph.flatMap(node => {
            val edges: List[String] = node._2.getEdges()
            val numOfEdges  = Double.valueOf(node._2.getNumOfEdges())
            val pageRank = node._2.getPageRank()
            val nextPageRank = alpha * pageRank / numOfEdges
            edges.map(nextNode => {
              val nextNodePageRank = new Page(null)
              nextNodePageRank.setPageRank(nextPageRank)
              (nextNode, nextNodePageRank)
            })
          }).reduceByKey(_+_)
          
          val constValue = graph.map(node => {
            val sumNode = new Page(node._2.getEdges())
            sumNode.setPageRank(zeroDegree)
            (node._1, sumNode)
          })
           
          val newGraph = sumGraph.union(constValue)
                                 .reduceByKey(_+_)
                                 .filter(node => 
                                 node._2.getEdges() != null)
          
          error = graph.join(newGraph).map(node => {
            node._2._1.getPageRankDiff(node._2._2.getPageRank())
          }).reduce(_+_)
          
          graph.unpersist()
          graph = newGraph
          graph.persist()
           
          println("[ERROR DIFF] " + String.valueOf(error) + ", Iteration = " + String.valueOf(numOfIteration))
        } 
 //
 //
        val finalGraph = graph.sortBy {
          node => (-node._2.getPageRank(), node._1)
        }
        val finalOutput = finalGraph.map(node => 
          String.valueOf(node._1) + "\t" + node._2.toString()
        )
        finalOutput.saveAsTextFile(outputPath)

        sc.stop
    }
}

class Page(lk: List[String]) extends Serializable {
  var link: List[String] = lk
  var pageRank: Double = 0.0
  var role: Int = 1

  override def toString(): String = {
    val linkStr = link match {
      case null => "(NULL)"
      case _ => link.toString()
    }
    //linkStr + "\t" + String.valueOf(pageRank) + "\t [ROLE] = " + String.valueOf(role)
    String.valueOf(pageRank)
  }
  def setPageRank(pg: Double): Unit = {
    pageRank = pg
  }
  def setEdges(edge: List[String]): Unit = {
    link = edge
  }
  def getPageRank(): Double = pageRank
  def getNumOfEdges(): Int = link.length
  def getEdges(): List[String] = link
  def +(that: Page): Page = {
    val sum = this.pageRank + that.pageRank
    var edge: List[String] = null
    if (this.getEdges() != null)  
      edge = this.getEdges()
    else if(that.getEdges() != null)
      edge = that.getEdges()

    val page = new Page(edge)
    page.setPageRank(sum)
    page
  }
  
  def ++(that: Page): Page = {
    var master = this
    var slave = that
    if (that.getRole() == 0) {
      master = that
      slave = this
    }
    val edges = master.getEdges() ::: slave.getEdges()
    master.setEdges(edges)
    master
  }

  def getPageRankDiff(pg: Double): Double = {
    new Double(Math.abs(this.pageRank  - pg)) 
  }
  
  def setMaster(): Unit = {
    this.role = 0
  }

  def getRole(): Int = this.role
}
