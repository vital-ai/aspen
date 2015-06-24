package ai.vital.aspen.analysis.pagerank

import ai.vital.aspen.analysis.training.AbstractModelTrainingTaskImpl
import ai.vital.aspen.groovy.predict.tasks.AssignPageRankValuesTask
import ai.vital.aspen.job.AbstractJob
import ai.vital.vitalsigns.VitalSigns
import ai.vital.aspen.model.PageRankPrediction
import ai.vital.aspen.groovy.predict.tasks.CalculateAggregationValueTask
import ai.vital.aspen.groovy.predict.tasks.CalculatePageRankValuesTask
import scala.collection.JavaConversions._
import ai.vital.aspen.analysis.training.ModelTrainingJob
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.vitalsigns.model.GraphObject
import org.apache.spark.SparkContext._

class AssignPageRankValuesTaskImpl(job: AbstractJob, task: AssignPageRankValuesTask) extends AbstractModelTrainingTaskImpl[AssignPageRankValuesTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
  }

  def execute(): Unit = {
    
    val trainRDD = job.getDataset(task.inputDatasetName)
    val m = trainRDD.collectAsMap()
    
    val nodeURI2Rank = task.getParamsMap.get(CalculatePageRankValuesTask.NODE_URI_2_RANK).asInstanceOf[Map[String, Double]]
    
    val thisModel = task.getModel
    
    val outputRDD = trainRDD.map { blockEncoded =>  
    
      val graphObjects = VitalSigns.get.decodeBlock(blockEncoded._2, 0, blockEncoded._2.length)
      
      val prPred = new PageRankPrediction()
      prPred.uri2Rank = nodeURI2Rank
      
      val results = thisModel.getModelConfig.getTarget.getFunction.call(new VitalBlock(graphObjects), null, prPred).asInstanceOf[java.util.List[GraphObject]]
      
      (blockEncoded._1, VitalSigns.get.encodeBlock(results))
      
    }
    
    if( job.isNamedRDDSupported() ) {
      
      job.namedRdds.update(task.outputDatasetName, outputRDD)
      
    } else {
      
      job.datasetsMap.put(task.outputDatasetName, outputRDD)
      
    }
    
    task.getParamsMap.put(task.outputDatasetName, outputRDD)
    
  }
}