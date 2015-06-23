package ai.vital.aspen.analysis.pagerank

import ai.vital.aspen.model.AspenPageRankPredictionModel
import ai.vital.aspen.analysis.training.AbstractTraining
import ai.vital.aspen.util.SetOnceHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.Graph
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import ai.vital.vitalsigns.VitalSigns
import scala.collection.JavaConversions._
import ai.vital.vitalsigns.model.VITAL_Edge
import ai.vital.vitalsigns.model.VITAL_Node
import java.util.ArrayList
import org.apache.spark.SparkContext._
import ai.vital.aspen.model.PageRankPrediction
import ai.vital.vitalsigns.block.CompactStringSerializer
import ai.vital.vitalsigns.model.GraphObject
import org.apache.hadoop.io.Text
import ai.vital.hadoop.writable.VitalBytesWritable

class AspenPageRankTraining(model: AspenPageRankPredictionModel) extends AbstractTraining[AspenPageRankPredictionModel](model) {
  
  def train(globalContext: java.util.Map[String, Object], trainRDD: RDD[(String, Array[Byte])]): java.io.Serializable = {
    
    trainRDD.cache()
    
    //first iteration to collect uri->id
    
    //broad
    val sc = trainRDD.sparkContext

    val nodes = trainRDD.flatMap { blockEncoded => 
      
      val graphObjects = VitalSigns.get.decodeBlock(blockEncoded._2, 0, blockEncoded._2.length)
      
      var uris = new ArrayList[(String, String)]
      
      for(x <- graphObjects) {
        if(x.isInstanceOf[VITAL_Node]) {
          if(!uris.contains(x.getURI)) {
            
            uris.add( ( x.getURI, x.toCompactString() ))    
          }
        }
      }

      uris
      
    }.reduceByKey((v1, v2) => v1)
    
    
    //generate uris index
    val nodesURIs = nodes.map { pair => pair._1}.collect()

    val nodesRDD : RDD[(VertexId, (String, String))] = nodes.map { pair => 
      ( nodesURIs.indexOf(pair._1).toLong, (pair._1, pair._2) )
    }

    val edgesRDD = trainRDD.flatMap { blockEncoded => 
      
      val graphObjects = VitalSigns.get.decodeBlock(blockEncoded._2, 0, blockEncoded._2.length)
      
      val edges = new ArrayList[Edge[String]]
      
      for(x <- graphObjects) {
        
        if(x.isInstanceOf[VITAL_Edge]) {
          val e = x.asInstanceOf[VITAL_Edge]
          val i1 = nodesURIs.indexOf(e.getSourceURI)
          val i2 = nodesURIs.indexOf(e.getDestinationURI)
          
          if(i1 >= 0 && i2 >= 0) {
            
        	  edges.add( Edge(i1, i2, e.getClass.getSimpleName) )
            
          }
          
          
        }
        
        
      }
      
      edges
      
    }
    
    val users : RDD[(VertexId, (String, String))] = null
    
    val missing = new VITAL_Node()
    missing.setURI("urn:missing")
    
    val defaultUser = ( missing.getURI, missing.toCompactString())
    
    val graph = Graph(nodesRDD, edgesRDD, defaultUser)
    
    val ranks = graph.pageRank(0.0001).vertices
    
    val thisModel = model
    
    val outputRDD = nodesRDD.join(ranks).map { j =>
      
      val rank = j._2._2
      
      val prPred = new PageRankPrediction()
      prPred.rank = rank
      prPred.node = CompactStringSerializer.fromString(j._2._1._2).asInstanceOf[VITAL_Node]
      
      val results = thisModel.getModelConfig.getTarget.getFunction.call(null, null, prPred).asInstanceOf[java.util.List[GraphObject]]
      
      (new Text(prPred.node.getURI), new VitalBytesWritable(VitalSigns.get.encodeBlock(results)))
      
//            val hadoopOutput = output.map( pair =>
//        (new Text(pair._1), new VitalBytesWritable(pair._2))
//      )
//      
      
    }
    
    outputRDD.saveAsSequenceFile(model.outputPath)
    
    null
    
  }
}