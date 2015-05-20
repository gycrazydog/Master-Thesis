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
object parallelSingleton {
//    def main(args:Array[String]){
//      val sparkConf = new SparkConf().setAppName("HBaseMultipleThread").setMaster("local")
//      val sc = new SparkContext(sparkConf)
//      val automata = GraphReader.automata(sc)
//      val startTime = System.currentTimeMillis 
//      var currentNodes = sc.parallelize(Array.range(1,3).map(i=>i.toLong))
//      var currentTrans = automata.edges.filter(e=>e.srcId==1L)
//      while(currentNodes.count()>0 && currentTrans.count()>0){
//        val currentEdges : RDD[Edge[String]] = currentTrans.flatMap(v=>GraphReader.getEdgesByLabel(sc, "Cells", v.attr).collect())
//        //currentEdges.collect().map(v=>println("edge reached!!! "+v))
//        currentTrans = currentTrans.cartesian(automata.edges).filter(v=>v._1.dstId==v._2.srcId).map(v=>v._2)
//        //currentTrans.collect().map(v=>println("trans reached!!! "+v))
//        currentNodes = currentEdges.map(e=>e.dstId)
//      }
//      val endTime = System.currentTimeMillis
//      currentNodes.collect().map(v=>println("vertex reached!!! "+v))
//      println("time : "+(endTime-startTime))
//    }
}