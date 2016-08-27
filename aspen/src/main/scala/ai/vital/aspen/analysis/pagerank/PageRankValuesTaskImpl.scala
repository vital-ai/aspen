package ai.vital.aspen.analysis.pagerank

import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.groovy.predict.tasks.PageRankValuesTask
import ai.vital.vitalsigns.VitalSigns
import java.util.ArrayList
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.graphx._
import scala.collection.JavaConversions._
import ai.vital.vitalsigns.model.VITAL_Edge
import ai.vital.vitalsigns.model.VITAL_Node
import ai.vital.aspen.analysis.training.AbstractModelTrainingTaskImpl
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.block.CompactStringSerializer
import ai.vital.domain.properties.Property_hasPageRank
import java.util.HashMap
import ai.vital.aspen.model.PageRankPrediction
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock

object PageRankValuesTaskImpl {
  
  // adapted from String.hashCode()
  // http://stackoverflow.com/questions/1660501/what-is-a-good-64bit-hash-function-in-java-for-textual-strings
  def hash( string: String) : Long = {
    
    var h = 1125899906842597L; // prime
    
    val len = string.length();

    var i = 0
    
    while ( i < len ) {
      
    	h = 31 * h + string.charAt(i);
    	
      i = i + 1
    }
    
    return h;
  }
  
}

class PageRankValuesTaskImpl(job: AbstractJob, task: PageRankValuesTask) extends AbstractModelTrainingTaskImpl[PageRankValuesTask](job.sparkContext, task) {
    
  def checkDependencies(): Unit = {
  
  }
  
  def execute(): Unit = {

    val trainRDD = job.getDataset(task.inputDatasetName)
    trainRDD.cache()
    
    val sc = trainRDD.sparkContext

    val unwrappedRDD = trainRDD.flatMap { blockEncoded =>
      
      val graphObjects = VitalSigns.get.decodeBlock(blockEncoded._2, 0, blockEncoded._2.length)
      
      val unwrapped = new ArrayList[(String, (String, String))]
      
      for( x <- graphObjects ) {
        
        unwrapped.add((x.getURI, (blockEncoded._1, x.toCompactString())));
        
      }
      
      unwrapped
      
    }
    
    unwrappedRDD.cache()
    
    val nodesRDD = unwrappedRDD.filter { row =>
      
      val g = CompactStringSerializer.fromString(row._2._2)
      
      g.isInstanceOf[VITAL_Node]
      
    }.map { row => 

        ( PageRankValuesTaskImpl.hash(row._1), row._1 )
    
    }.reduceByKey{ (s1, s2) => 
       s1
    }
    
    
    val edgesRDD : RDD[Edge[String]] = unwrappedRDD.filter { row =>
      
      val g = CompactStringSerializer.fromString(row._2._2)
      
      g.isInstanceOf[VITAL_Edge]
      
    }.map { row =>
      
      val g = CompactStringSerializer.fromString(row._2._2)
      
      val e = g.asInstanceOf[VITAL_Edge]
      val i1 = PageRankValuesTaskImpl.hash(e.getSourceURI)
      val i2 = PageRankValuesTaskImpl.hash(e.getDestinationURI)
      
      Edge(i1, i2, e.getClass.getSimpleName)
      
    }
    
    
    val missing = new VITAL_Node()
    missing.setURI("urn:missing")
    
    val defaultUser = missing.getURI
    
    val graph = Graph(nodesRDD, edgesRDD, defaultUser)
    
    val ranks = graph.pageRank(0.0001).vertices
    
    val nodeURI2Rank = nodesRDD.join(ranks).map { j =>
      
      val rank = j._2._2
      
      val nodeURI = j._2._1
      
      (nodeURI, rank)
      
    }
    
    
    
    val jointRDD = unwrappedRDD.leftOuterJoin(nodeURI2Rank)
    val blockURI2CompactString = jointRDD.map { rowScore => 
      
      val blockURIGraphObjectScore = rowScore._2
      
      var score : java.lang.Double = null;
      
      if( !blockURIGraphObjectScore._2.isEmpty ) {
        score = blockURIGraphObjectScore._2.get
      }
      
      var outputCompactString = blockURIGraphObjectScore._1._2

      val blockURI = blockURIGraphObjectScore._1._1
      
      (blockURI, ( outputCompactString, score ))
      
    }
    
    val blocksGrouped = blockURI2CompactString.groupByKey()
 
    val thisModel = task.getModel
    
    val outputRDD = blocksGrouped.map { block =>
      
      val graphObjects = new ArrayList[GraphObject]//VitalSigns.get.decodeBlock(blockEncoded._2, 0, blockEncoded._2.length)
      
      val uri2Score = new HashMap[String, Double]();
      
      for( s <- block._2 ) {
        
        val g = CompactStringSerializer.fromString(s._1)
        
        graphObjects.add( g )
       
        if(s._2 != null) {
          uri2Score.put(g.getURI(), s._2)
        }
        
      }
      
      val prPred = new PageRankPrediction()
      prPred.uri2Rank = uri2Score
      
      val results = thisModel.getModelConfig.getTarget.getFunction.call(new VitalBlock(graphObjects), null, prPred).asInstanceOf[java.util.List[GraphObject]]
      
      if(results != null && results.size() > 0 ) {
    	  (block._1, VitalSigns.get.encodeBlock(results))
      } else {
        null
      }
      
    }.filter( pair => pair != null )

    
    
    if( job.isNamedRDDSupported() ) {
      
      job.namedRdds.update(task.outputDatasetName, outputRDD)
      
    } else {
      
      job.datasetsMap.put(task.outputDatasetName, outputRDD)
      
    }
    
    task.getParamsMap.put(task.outputDatasetName, outputRDD)
    
  }
  
}