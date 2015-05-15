package regularPathQuery
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
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
import scala.collection.JavaConverters._
object sequentialSingleton {
    def main(args: Array[String]){
      val sparkConf = new SparkConf().setAppName("HBaseDistributedScanExample " ).setMaster("local")
      val sc = new SparkContext(sparkConf)
      val graph = GraphReader.read(sc)
      val automata = GraphReader.automata(sc)
      val startTime = System.currentTimeMillis 
      var currentNodes = graph.vertices.map(v=>v._1)
      var currentTrans = automata.edges.filter(e=>e.srcId==1L)
      while(currentNodes.count()>0 && currentTrans.count()>0){
        val nextEdges = graph.edges.cartesian(currentNodes).filter(v=>v._1.srcId==v._2).map(v=>v._1)
        val currentEdges = nextEdges.cartesian(currentTrans).filter(v=>v._1.attr==v._2.attr).map(v=>v._1)
        //currentEdges.collect().map(v=>println("edge reached!!! "+v))
        currentTrans = currentTrans.cartesian(automata.edges).filter(v=>v._1.dstId==v._2.srcId).map(v=>v._2)
        //currentTrans.collect().map(v=>println("trans reached!!! "+v))
        currentNodes = currentEdges.map(e=>e.dstId)
      }
      val endTime = System.currentTimeMillis
      currentNodes.collect().map(v=>println("vertex reached!!! "+v))
      println("time : "+(endTime-startTime))
    }
}