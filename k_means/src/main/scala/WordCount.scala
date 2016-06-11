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
        val parser = lines.map(line => {
          val patterns = line.split(",")
          val key = patterns(2) + "," + patterns(0).substring(0, 6)
          val price = Double.valueOf(patterns(patterns.length - 2));
          val quantity = Double.valueOf(patterns(patterns.length - 1));
          val value = new Data(price, quantity, Double.valueOf(1.0))
          (key, value)
        }).reduceByKey(_+_) 

        val dataSet = parser.map(pair => {
          val patterns = pair._1.split(",")
          val name = patterns(0)
          val date = patterns(1)
          val price = pair._2.getPrice()
          val quantity = pair._2.getQuantity()
          val dataSet = new DataSet(date, price, quantity)
          (name, dataSet) 
        }).reduceByKey(_++_).filter(_._2.size() == 53).map(pair => { 
          pair._2.setSlopeFunc()
          pair
        })
        
        var currentSet = dataSet.sortBy {
          pair => (pair._1, pair._2.toString())
        }
        
        /* Below do clustering */

        val coreNum = 20
        var core = currentSet.takeSample(false, coreNum)
        var current = 0
        var last = Double.valueOf(0)
        while (current < 20) {
          /* Step 1: assign each vector to most closed cluster */
          val newData = currentSet.map(nodePair => {
            val node = nodePair._2 

            var currentIndex = 0
            var currentDis = node.getDistance(core(0)._2)
            for (i <- 1 to coreNum - 1) {
              var otherDis = node.getDistance(core(i)._2)
              if (currentDis.compareTo(otherDis) > 0) {
                currentIndex = i
                currentDis = otherDis 
              }
            }
            node.setGroupID(currentIndex)
            node.setGroupDis(currentDis)
            (nodePair._1, node)
          }).cache()
        
          var distance = newData.map(_._2.getGroupDis()).reduce(_+_)
          print("[Distance " + String.valueOf(current)  + "] = " + String.valueOf(distance) + "\n")
           
          var newCore = newData.map(pair => {
            var node = pair._2
            node.setVecNum(1)
            (node.getGroupID(), node)
          }).reduceByKey(_+++_)
          .map(pair => {
            val updatedNode = pair._2
            updatedNode.updateVector()
            (pair._1, updatedNode)     
          }).sortBy(_._1)
          .map(pair => (String.valueOf(pair._1), pair._2)).toArray
          
          currentSet = newData
          core = newCore
          newData.unpersist()
          
          current = current + 1
        }

        currentSet.saveAsTextFile(outputPath)
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
  var vector =  scala.collection.mutable.ArrayBuffer[Double]()
  var vecNum = 0
  var groupID = 0
  var groupDis = Double.valueOf(0)

  override def toString(): String = {
    val sortPriceSet = priceSet.toSeq.sortBy(_._1)
    val sortQuantitySet = quantitySet.toSeq.sortBy(_._1)
    var output = String.valueOf(this.vector.size)
/*
    sortPriceSet.map(price => {
      output = output + ", " + price._2
    })
    
    sortQuantitySet.map(quantity => {
      output = output + ", " + quantity._2
    })
*/   
    for (slope <- vector) 
      output = output + ", " + slope
    output = output + ", GROUP = " + groupID
    output
  }

  def ++(that: DataSet): DataSet = {
    this.priceSet = this.priceSet ++ that.priceSet
    this.quantitySet = this.quantitySet ++ that.quantitySet
    this
  }
  
  def +++(that: DataSet): DataSet = {
    for (i <- 0 to this.vector.size - 1) 
      this.vector(i) = this.vector(i) + that.vector(i)
    
    /* GroupID here used to get number node of cluster */
    this.vecNum = this.vecNum + that.vecNum
    this
  }
  
  def updateVector() = {
    for (i <- 0 to this.vector.size - 1) {
      this.vector(i) = this.vector(i) / this.vecNum
    }
  }
  
  def getVecNum(): Int = {
    this.vecNum
  }

  def setVecNum(number: Int) = {
    this.vecNum = number
  }

  def getDistance(that: DataSet): Double = {
    var sum = new Double(0)
    for (i <- 0 to this.vector.size - 1) {
      val diff = this.vector(i) - that.vector(i)
      sum = sum + diff * diff
    }
    sum
  }
  
  def setGroupDis(distance: Double) = {
    this.groupDis = distance
  }
  
  def getGroupDis(): Double = {
    this.groupDis
  }

  def size(): Int = {
    this.priceSet.size
  }
  
  def getGroupID(): Int = {
    this.groupID
  }

  def setGroupID(id: Int) = {
    this.groupID = id
  }

  def setSlopeFunc() {
    val sortPriceSet = priceSet.toSeq.sortBy(_._1)
    val sortQuantitySet = quantitySet.toSeq.sortBy(_._1)
    var i = 0
    
    val priceVec =  scala.collection.mutable.ArrayBuffer[Double]()
    val quantityVec = scala.collection.mutable.ArrayBuffer[Double]() 
     
    for (i <- 1 to sortPriceSet.size - 1)
      priceVec.append(sortPriceSet(i)._2 - sortPriceSet(i - 1)._2)
    
    for (i <- 1 to sortQuantitySet.size - 1)
      quantityVec.append(sortQuantitySet(i)._2 - sortQuantitySet(i - 1)._2)
    
    val maxPrice = priceVec.max
    val minPrice = priceVec.min
    val maxQuantity = quantityVec.max
    val minQuantity = quantityVec.min
    
    val priceScalar = 2.0 / (maxPrice - minPrice)
    val priceOffset = -1.0 - (minPrice * priceScalar)
    val quantityScalar = 2.0 / (maxQuantity - minQuantity) 
    val quantityOffset = -1.0 - (minQuantity * quantityScalar)

    for (price <- priceVec) 
      vector.append(price * priceScalar + priceOffset)
    
    for (q <- quantityVec)
      vector.append(q * quantityScalar + quantityOffset)
  }
}
