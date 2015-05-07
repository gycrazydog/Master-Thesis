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
object GraphReader {
  def read(sc : SparkContext):Graph[Any,String] = {
      val tableName = "TestGraph";
  
      val conf = HBaseConfiguration.create()
      conf.set(TableInputFormat.INPUT_TABLE,tableName)
  
      val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], 
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])
      hBaseRDD.foreach(v => {
        print(Bytes.toString(v._1.get()))
        val result = v._2
        val it = result.listCells().iterator()
        while(it.hasNext()){
          val kv = it.next
          print(" "+Bytes.toString(kv.getQualifier)+" "+Bytes.toString(kv.getValue))
        }
        println()
      })
      var users : RDD[(VertexId,(Any))]= hBaseRDD.map( v=> (Bytes.toString(v._1.get()).toLong,())) 
      
      println("before flatmap!")
      var relationships = hBaseRDD.flatMap(v=> {
        val result : Result = v._2
        var res : Array[Edge[String]] = Array()
        val pp  = result.listCells.asScala.map ( cell => 
          if(Bytes.toString(cell.getQualifier)!="")
            Edge(Bytes.toString(v._1.get()).toLong, Bytes.toString(cell.getValue).toLong,Bytes.toString(cell.getQualifier)) 
          else
            Edge(Bytes.toString(v._1.get()).toLong, Bytes.toString(v._1.get()).toLong,Bytes.toString(cell.getQualifier)) 
        )
        pp  
      })
      val edges = relationships.filter(edge => edge.attr!="")
      val defaultUser = ("John Doe", "Missing")
      // Build the initial Graph
      val graph = Graph(users, edges, defaultUser)
      graph
  }
  def automata(sc : SparkContext) : Graph[Any,String] = {
     val states : RDD[(VertexId,(Any))]= sc.parallelize(Array(
                      (1L, ()), 
                      (2L, ()),
                       (3L, ())
                       ))
      val trans = sc.parallelize(Array(
                    Edge(1L, 2L, "category"),    
                    Edge(2L, 3L, "music")
                  ))
      val defaultUser = ("John Doe", "Missing")
      val graph = Graph(states, trans,defaultUser)
      graph
  }
}