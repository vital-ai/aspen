package ai.vital.aspen.analysis.pagerank

import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.groovy.predict.tasks.CalculatePageRankValuesTask
import ai.vital.vitalsigns.VitalSigns
import java.util.ArrayList
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.graphx._
import scala.collection.JavaConversions._
import ai.vital.vitalsigns.model.VITAL_Edge
import ai.vital.vitalsigns.model.VITAL_Node
import ai.vital.aspen.analysis.training.AbstractModelTrainingTaskImpl

class CalculatePageRankValuesTaskImpl(job: AbstractJob, task: CalculatePageRankValuesTask) extends AbstractModelTrainingTaskImpl[CalculatePageRankValuesTask](job.sparkContext, task) {
    
  def checkDependencies(): Unit = {
  
  }

  def execute(): Unit = {

    val trainRDD = job.getDataset(task.inputDatasetName)
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
    
    val nodeURI2Rank = nodesRDD.join(ranks).map { j =>
      
      val rank = j._2._2
      
      val nodeURI = j._2._1
      
      (nodeURI, rank)
      
    }.collectAsMap().toMap
    
    task.getParamsMap.put(CalculatePageRankValuesTask.NODE_URI_2_RANK, nodeURI2Rank)
    
  }
}