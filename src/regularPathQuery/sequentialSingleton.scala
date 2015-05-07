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
      var currentNodes = graph.vertices.map(v=>v._1)
      var currentTrans = automata.edges.filter(e=>e.srcId==1L)
      while(currentNodes.count()>0 && currentTrans.count()>0){
        val tempEdges = graph.edges.cartesian(currentTrans).filter{case(a,b)=>a.attr==b.attr}
        var currentEdges = tempEdges.map{case(a,b)=>a}
        //currentEdges.collect().map(v=>println("edge reached!!! "+v))
        val tempTrans = currentTrans.cartesian(automata.edges).filter{case (a,b)=>a.dstId==b.srcId}
        currentTrans = tempTrans.map{case (a,b)=>b}
        //currentTrans.collect().map(v=>println("trans reached!!! "+v))
        currentNodes = currentEdges.map(e=>e.dstId)
      }
      currentNodes.collect().map(v=>println("vertex reached!!! "+v))
      
    }
}