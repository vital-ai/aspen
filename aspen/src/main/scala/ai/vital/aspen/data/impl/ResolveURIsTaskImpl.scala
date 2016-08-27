package ai.vital.aspen.data.impl

import ai.vital.aspen.analysis.training.AbstractModelTrainingTaskImpl
import org.apache.spark.SparkContext
import ai.vital.aspen.groovy.data.tasks.LoadDataSetTask
import org.apache.spark.SparkContext
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
import ai.vital.aspen.groovy.data.tasks.ResolveURIReferencesTask
import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.groovy.predict.ModelTrainingTask
import ai.vital.aspen.groovy.predict.tasks.TrainModelTask
import ai.vital.aspen.groovy.modelmanager.AspenModel
import ai.vital.aspen.groovy.predict.tasks.LoadModelTask
import ai.vital.vitalsigns.meta.GraphContext
import ai.vital.vitalsigns.model.property.URIProperty
import ai.vital.vitalservice.VitalService

class ResolveURIsTaskImpl(job: AbstractJob, task: ResolveURIReferencesTask) extends TaskImpl[ResolveURIReferencesTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
    if(job.isNamedRDDSupported()) {
//      if( this.namedRdds.get(ldt.datasetName).isDefined ) throw new RuntimeException("Dataset already loaded: " + ldt.datasetName)
    } else {
//      if( datasetsMap.get(ldt.datasetName) != null) throw new RuntimeException("Dataset already loaded: " + ldt.datasetName);
    }
  }

  def execute(): Unit = {
    
    val datasetName = task.datasetName
    
    var inputBlockRDD = job.getDataset(datasetName)
    
    if(inputBlockRDD == null) throw new RuntimeException("Dataset not found: " + datasetName)

    val serviceProfile_ = job.serviceProfile_
    
    val serviceConfig = job.serviceConfig
    
    val serviceKey = job.serviceKey

    inputBlockRDD = inputBlockRDD.map { pair =>
            
      //make sure the URIReferences are resolved
      val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
      val vitalBlock = new VitalBlock(inputObjects)
            
      if(vitalBlock.getMainObject.isInstanceOf[URIReference]) {
              
         if( VitalSigns.get.getVitalService == null ) {
        	 var vitalService : VitalService = null
           if(serviceProfile_ != null) {
        	   vitalService= VitalServiceFactory.openService(serviceKey, serviceProfile_)
           } else {
             vitalService = VitalServiceFactory.openService(serviceKey, serviceConfig)
           }
           VitalSigns.get.setVitalService(vitalService)
         }
         
         val uriRef = vitalBlock.getMainObject.asInstanceOf[URIReference];
         
         val go = VitalSigns.get.getVitalService.get(GraphContext.ServiceWide, URIProperty.withString(uriRef.getProperty("uRIRef").toString())).first()

         
         if(go != null) {
           
           vitalBlock.setMainObject(go)
           
         }
         
         (pair._1, VitalSigns.get.encodeBlock(vitalBlock.toList()))
              
       } else {
              
          (pair._1, pair._2)
       }        
//        inputBlockRDD.cache()
          
    }
    
    
    if(job.isNamedRDDSupported()) {
      job.namedRdds.update(task.datasetName, inputBlockRDD);
    } else {
      job.datasetsMap.put(task.datasetName, inputBlockRDD)
    }
  
  }
  
  
}