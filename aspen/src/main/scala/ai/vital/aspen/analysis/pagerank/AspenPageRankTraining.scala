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
import org.apache.hadoop.fs.FileSystem
import ai.vital.aspen.analysis.training.ModelTrainingJob
import org.apache.hadoop.fs.Path
import ai.vital.vitalsigns.block.BlockCompactStringSerializer
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.io.BufferedWriter
import ai.vital.aspen.groovy.predict.tasks.TrainModelTask
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock

class AspenPageRankTraining(model: AspenPageRankPredictionModel, task: TrainModelTask) extends AbstractTraining[AspenPageRankPredictionModel](model) {
  
  def train(globalContext: java.util.Map[String, Object], trainRDD: RDD[(String, Array[Byte])]): java.io.Serializable = {
    
    trainRDD.cache()
    
    //first iteration to collect uri->id
    
    //broad
    val sc = trainRDD.sparkContext

    //generate uris index
    val nodesURIs = trainRDD.flatMap { blockEncoded => 
      
      val graphObjects = VitalSigns.get.decodeBlock(blockEncoded._2, 0, blockEncoded._2.length)
      
      var uris = new ArrayList[String]
      
      for(x <- graphObjects) {
        if(x.isInstanceOf[VITAL_Node]) {
          if(!uris.contains(x.getURI)) {
            
            uris.add( x.getURI )    
          }
        }
      }

      uris
      
    }.distinct().collect()
    
    
    val nodesURIsList = new ArrayList[(VertexId, String)]
    var c = 0L
    for(x <- nodesURIs) {
      nodesURIsList.add((c, x))
      c = c+1
    }
    
    val nodesRDD : RDD[(VertexId, String)] = sc.parallelize(nodesURIsList.toSeq)

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
    
    val defaultUser = missing.getURI
    
    val graph = Graph(nodesRDD, edgesRDD, defaultUser)
    
    val ranks = graph.pageRank(0.0001).vertices
    
    val thisModel = model
    
    val nodeURI2Rank = nodesRDD.join(ranks).map { j =>
      
      val rank = j._2._2
      
      val nodeURI = j._2._1
      
      (nodeURI, rank)
      
    }.collectAsMap()
    
    val outputRDD = trainRDD.map { blockEncoded =>  
    
      val graphObjects = VitalSigns.get.decodeBlock(blockEncoded._2, 0, blockEncoded._2.length)
      
      val prPred = new PageRankPrediction()
      prPred.uri2Rank = nodeURI2Rank
      
      val results = thisModel.getModelConfig.getTarget.getFunction.call(new VitalBlock(graphObjects), null, prPred).asInstanceOf[java.util.List[GraphObject]]
      
      graphObjects.addAll(results)
      
      
      (blockEncoded._1, VitalSigns.get.encodeBlock(graphObjects))
      
    }
    
    if( ModelTrainingJob.isNamedRDDSupported() ) {
      
      ModelTrainingJob.namedRdds.update(task.outputDatasetName, outputRDD)
      
    } else {
      
      ModelTrainingJob.datasetsMap.put(task.outputDatasetName, outputRDD)
      
    }
    
    task.getParamsMap.put(task.outputDatasetName, outputRDD)
    
    null
    
  }
}