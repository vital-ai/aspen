package ai.vital.aspen.data.impl

import ai.vital.aspen.analysis.training.AbstractModelTrainingTaskImpl
import org.apache.spark.SparkContext
import ai.vital.aspen.groovy.data.tasks.LoadDataSetTask
import org.apache.spark.SparkContext
import ai.vital.aspen.analysis.training.ModelTrainingJob
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import ai.vital.hadoop.writable.VitalBytesWritable
import org.apache.hadoop.io.Text
import ai.vital.vitalsigns.VitalSigns
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.vitalsigns.model.URIReference
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.groovy.predict.ModelTrainingTask
import ai.vital.aspen.groovy.predict.tasks.TrainModelTask
import ai.vital.aspen.groovy.modelmanager.AspenModel
import ai.vital.aspen.groovy.predict.tasks.LoadModelTask
import ai.vital.aspen.groovy.predict.tasks.FeatureQueryTask
import java.util.ArrayList
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalservice.VitalService

class FeatureQueryTaskImpl(job: AbstractJob, task: FeatureQueryTask) extends TaskImpl[FeatureQueryTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
    if(ModelTrainingJob.isNamedRDDSupported()) {
//      if( this.namedRdds.get(ldt.datasetName).isDefined ) throw new RuntimeException("Dataset already loaded: " + ldt.datasetName)
    } else {
//      if( datasetsMap.get(ldt.datasetName) != null) throw new RuntimeException("Dataset already loaded: " + ldt.datasetName);
    }
  }

  def execute(): Unit = {
    
    val datasetName = task.datasetName
    
    var inputBlockRDD = job.getDataset(datasetName)
    
    if(inputBlockRDD == null) throw new RuntimeException("Dataset not found: " + datasetName)

    //find model
    var modelO : Object = null
    
    modelO = task.getParamsMap.get("trained-model")
    
    if(modelO == null) {
      modelO = task.getParamsMap.get(LoadModelTask.LOADED_MODEL_PREFIX + task.modelPath);
    }
    
    if(modelO == null) throw new RuntimeException("No model loaded, neither in training nor in load step")
    
    val model = modelO.asInstanceOf[AspenModel]
    
    val fqs = model.getModelConfig.getFeatureQueries
    val tqs = model.getModelConfig.getTrainQueries
    
    if((fqs == null || fqs.size() == 0) && (tqs == null || tqs.size() == 0)) {
      task.getParamsMap.put(FeatureQueryTask.FEATURE_QUERY_PREFIX + datasetName, new java.lang.Boolean(true))
      return
    }
    
    val serviceProfile_ = job.serviceProfile_

    val serviceConfig = job.serviceConfig
    
    val serviceKey = job.serviceKey
    
    inputBlockRDD = inputBlockRDD.map { pair =>
            
      //make sure the URIReferences are resolved
      val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
      val vitalBlock = new VitalBlock(inputObjects)
            
      
      //force load data 
      if(!vitalBlock.getMainObject.isInstanceOf[URIReference]) {
        val uriRef = new URIReference()
        uriRef.setURI("urn:" + System.currentTimeMillis())
        uriRef.setProperty("uRIRef", vitalBlock.getMainObject.getURI)
        vitalBlock.setMainObject(uriRef)
        vitalBlock.setDependentObjects(new ArrayList[GraphObject]())
      }
      
      if(vitalBlock.getMainObject.isInstanceOf[URIReference]) {
              
         if( VitalSigns.get.getVitalService == null ) {
           var vitalService : VitalService = null;
           if(serviceProfile_ != null ) {
        	   vitalService = VitalServiceFactory.openService(serviceKey, serviceProfile_)
           } else {
             vitalService = VitalServiceFactory.openService(serviceKey, serviceConfig)
           }
           VitalSigns.get.setVitalService(vitalService)
         }
              
         //loads objects from features queries and train queries 
         model.getFeatureExtraction.composeBlock(vitalBlock)
              
         (pair._1, VitalSigns.get.encodeBlock(vitalBlock.toList()))
              
       } else {
              
          (pair._1, pair._2)
       }        
//        inputBlockRDD.cache()
          
    }
    
    
    if(ModelTrainingJob.isNamedRDDSupported()) {
      ModelTrainingJob.namedRdds.update(task.datasetName, inputBlockRDD);
    } else {
      ModelTrainingJob.datasetsMap.put(task.datasetName, inputBlockRDD)
    }
    
    task.getParamsMap.put(FeatureQueryTask.FEATURE_QUERY_PREFIX + datasetName, new java.lang.Boolean(true))
  
  }
  
  
}