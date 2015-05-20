package regularPathQuery
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.graphx.PartitionStrategy._
import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.slf4j.impl.Log4jLoggerFactory
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.collection.immutable.HashSet
import scala.collection.JavaConverters._
object sequentialSingleton {
    
    def main(args: Array[String]){
      
//      val graph = GraphReader.read(sc)
      multipleThread(4)
    }
    def singleThread():Unit = {
      val sparkConf = new SparkConf().setAppName("HBaseMultipleThread").setMaster("local")
      val sc = new SparkContext(sparkConf)
      val automata = GraphReader.automata(sc)
      val finalState = HashSet(3L)
      val startTime = System.currentTimeMillis 
      var ans : Array[VertexId] = Array()
      var currentTrans = automata.edges.filter(e=>e.srcId==1L)
      currentTrans.cache()
      val firstEdges = GraphReader.getEdgesByLabels(sc,"Cells",currentTrans.map(e=>e.attr).collect())
      firstEdges.cache()
      var currentStates : RDD[(Edge[String],Edge[String])] = currentTrans.cartesian(firstEdges).filter(f=>f._1.attr.equals(f._2.attr))
      currentStates.cache()
      var i = 0
      while(currentStates.count()>0){
        i = i+1
        println("iteration:"+i)
//        println("current States:")
//        currentStates.collect().map(println)
        ans = ans ++ currentStates.filter(v=>finalState.contains(v._1.dstId)).map(v=>v._2.dstId).collect()
        currentTrans = currentTrans.cartesian(automata.edges).filter(v=>v._1.dstId==v._2.srcId).map(v=>v._2)
        currentTrans.cache()
        val nextEdges = GraphReader.getEdgesByIds(sc, currentStates.map(f=>f._2).map(e=>e.dstId))
        nextEdges.cache()
        val nextStates = currentTrans.cartesian(nextEdges).filter(f=>f._1.attr.equals(f._2.attr))
        nextStates.cache()
        currentStates = currentStates.cartesian(nextStates).filter(f=>{
          val currentState = f._1
          val nextState = f._2
          (currentState._1.dstId==nextState._1.srcId)&&(currentState._2.dstId==nextState._2.srcId)
        }).map(s=>s._2)
        currentStates.cache()
      }
      val endTime = System.currentTimeMillis
      ans.map(v=>println("vertex reached!!! "+v))
      println("time : "+(endTime-startTime))
    }
    
    def multipleThread(workerNum:Int):Unit = {
      val sparkConf = new SparkConf().setAppName("HBaseMultipleThread").setMaster("local["+workerNum+"]")
      val sc = new SparkContext(sparkConf)
      val automata = GraphReader.automata(sc)
      val finalState = HashSet(3L)
      val startTime = System.currentTimeMillis 
      var ans : Array[VertexId] = Array()
      var currentTrans = automata.edges.filter(e=>e.srcId==1L)
      currentTrans.cache()
      val firstEdges = GraphReader.getEdgesByLabels(sc,"Cells",currentTrans.map(e=>e.attr).collect())
      firstEdges.cache()
      var currentStates : RDD[(Edge[String],Edge[String])] = currentTrans.cartesian(firstEdges).filter(f=>f._1.attr.equals(f._2.attr)).repartition(workerNum)
      currentStates.cache()
      var visited : HashSet[(VertexId,VertexId)] = new HashSet()
      println("count : " +currentStates.count())
      var i = 0
      while(currentStates.count()>0){
        i = i+1
        visited ++= currentStates.map(s=>(s._1.dstId,s._2.dstId)).collect()
        println("iteration: "+i)
        println("visited: "+visited.size)
//        println("current States:")
//        currentStates.collect().map(println)
        ans = ans ++ currentStates.filter(v=>finalState.contains(v._1.dstId)).map(v=>v._2.dstId).collect()
//        var currentNodes = currentStates.map(f=>f._2).map(e=>e.dstId)
        currentTrans = currentTrans.cartesian(automata.edges).filter(v=>v._1.dstId==v._2.srcId).map(v=>v._2).repartition(workerNum)
        currentTrans.cache()
        val nextEdges = GraphReader.getEdgesByIds(sc, currentStates.map(f=>f._2).map(e=>e.dstId))
        nextEdges.cache()
        val nextStates = currentTrans.cartesian(nextEdges).filter(f=>f._1.attr.equals(f._2.attr)).repartition(workerNum)
        nextStates.cache()
        currentStates = currentStates.cartesian(nextStates).filter(f=>{
          val currentState = f._1
          val nextState = f._2
          (currentState._1.dstId==nextState._1.srcId)&&(currentState._2.dstId==nextState._2.srcId)&&(!visited.contains((nextState._1.dstId,nextState._2.dstId)))
        }).map(s=>s._2).repartition(workerNum)
        currentStates.cache()
      }
      val endTime = System.currentTimeMillis
      ans.map(v=>println("vertex reached!!! "+v))
      println("time : "+(endTime-startTime))
    }
}