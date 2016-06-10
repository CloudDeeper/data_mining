/* WordCount.scala */
import org.apache.spark._
import org.apache.hadoop.fs._
import java.util.regex._
// import java.util._
import java.lang.Double
import java.lang.Math
import scala.collection.mutable.ListBuffer

object WordCount {
    def main(args: Array[String]) {

        val files = "final/raw_data"
        val outputPath = "final/statistic_data"
        val conf = new SparkConf().setAppName("Economics Statistic")
        val sc = new SparkContext(conf)
        
        // Cleanup output dir
        val hadoopConf = sc.hadoopConfiguration
        val hdfs = FileSystem.get(hadoopConf)
        try { hdfs.delete(new Path(outputPath), true) } catch { case _ : Throwable => { } }
        
        val lines = sc.textFile(files)
/*
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
*/
        val parser = lines.map(line => {
          val patterns = line.split(",")
          val key = patterns(2) + "," + patterns(0).substring(0, 6)
          val price = Double.valueOf(patterns(patterns.length - 2));
          val quantity = Double.valueOf(patterns(patterns.length - 1));
          val value = new Data(price, quantity, Double.valueOf(1.0))
          (key, value)
        }).reduceByKey(_+_) 
/* 
        val dataSet = parser.map(pair => {
          val patterns = pair._1.split(" ")
          val name = patterns(0)
          val date = patterns(1)
          val price = pair._2.getPrice()
          val quantity = pair._2.getQuantity()
          val dataSet = new DataSet(date, price, quantity)
          (name, dataSet) 
        }).reduceByKey(_++_)
        val result = dataSet.sortBy {
          pair => (pair._1, pair._2.toString())
        }
*/
        val sortSet = parser.sortBy {
          pair => (pair._1, pair._2.toString())
        }
        
        val result = sortSet.map(pair => {
          pair._1 + "," + pair._2.toString()
        })

        result.saveAsTextFile(outputPath)
        sc.stop
    }
}

class Data(_price: Double, _quantity: Double, _count: Double) extends Serializable {
  var price: Double = _price
  var quantity: Double = _quantity
  var count: Double = _count
   
  override def toString(): String = {
    String.valueOf(_price/Double.valueOf(_count)) + "," +  String.valueOf(_quantity)
  }
  
  def +(that: Data): Data = { 
    val newPrice = this.price + that.price
    val newQuantity = this.quantity + that.quantity
    val newCount = this.count + that.count
    val newData = new Data(newPrice, newQuantity, newCount)
    newData
  }

  def getPrice(): Double = {
    this.price / this.count
  }

  def getQuantity(): Double = {
    this.quantity
  }
}

class DataSet(date: String, price: Double, quantity: Double) extends Serializable {
  var priceSet = Map((date, price))
  var quantitySet = Map((date, quantity))
  
  override def toString(): String = {
    var output = ""
    priceSet.map(price => {
      output = output + ", " + price
    })
    output
  }

  def ++(that :DataSet): DataSet = {
    this.priceSet = this.priceSet ++ that.priceSet
    this.quantitySet = this.quantitySet ++ that.quantitySet
    this
  }
}
