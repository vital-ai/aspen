package ai.vital.aspen.analysis.predict.impl

import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.groovy.predict.tasks.ModelPredictTask
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.groovy.predict.tasks.LoadModelTask
import ai.vital.aspen.groovy.modelmanager.AspenModel
import ai.vital.vitalsigns.VitalSigns
import scala.collection.JavaConversions._
import ai.vital.vitalsigns.model.GraphObject
import java.util.ArrayList

class ModelPredictTaskImpl(job: AbstractJob, task: ModelPredictTask) extends TaskImpl[ModelPredictTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
    
  }

  def execute(): Unit = {
    
    var modelO = task.getParamsMap.get(LoadModelTask.LOADED_MODEL_PREFIX + task.modelPath)
    
    if(modelO == null) throw new RuntimeException("Loaded model not found, path: " + task.modelPath)
    
    if(!modelO.isInstanceOf[AspenModel]) throw new RuntimeException("Invalid object type: " + modelO.getClass.getCanonicalName)

    val aspenModel = modelO.asInstanceOf[AspenModel]
    
    val inputRDD = job.getDataset(task.inputDatasetName)
    
    val acc = job.sparkContext.accumulator(0)
    
    val outputRDD = inputRDD.map { pair => 
    
      val key = pair._1
      
      val graphObjects = VitalSigns.get.decodeBlock(pair._2, 0, pair._2.length)
      
      val outputObjects = aspenModel.predict(graphObjects)
      
      val uris = new java.util.HashSet[String]()
      
      for(g <- outputObjects) {
        uris.add(g.getURI)
      }
      
      val targetList = new ArrayList[GraphObject]()
      
      for(g <- graphObjects) {
        
        if(!uris.contains(g.getURI)) targetList.add(g)
        
      }
      
      targetList.addAll(outputObjects)
     
      acc.add(1)
      
      (key, VitalSigns.get.encodeBlock(targetList))
      
    }
    
    if( job.isNamedRDDSupported() ) {
      
      job.namedRdds.update(task.outputDatasetName, outputRDD)
      
    } else {
      
      job.datasetsMap.put(task.outputDatasetName, outputRDD)
      
    }
    
    task.getParamsMap.put(ModelPredictTask.STATS_STRING, "Processsed " + acc.value + " blocks")
    task.getParamsMap.put(task.outputDatasetName, outputRDD)
    
  }
}