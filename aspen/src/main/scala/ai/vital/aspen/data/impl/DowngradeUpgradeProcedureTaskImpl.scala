package ai.vital.aspen.data.impl

import ai.vital.aspen.data.DowngradeUpgradeProcedureTask
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.data.LoaderSingleton
import ai.vital.vitalsigns.domains.DifferentDomainVersionLoader
import ai.vital.vitalservice.impl.UpgradeDowngradeProcedure
import ai.vital.vitalsigns.binary.VitalSignsBinaryFormat
import ai.vital.vitalsigns.VitalSigns
import scala.collection.JavaConversions._
import java.util.ArrayList
import ai.vital.vitalsigns.model.GraphObject

class DowngradeUpgradeProcedureTaskImpl(job: AbstractJob, task: DowngradeUpgradeProcedureTask) extends TaskImpl[DowngradeUpgradeProcedureTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
  
  }

  def execute(): Unit = {

    var inputRDD = job.getDataset( task.getInputDatasetName )
    
    val singleton = LoaderSingleton.get
    
    val mainDomainBytes = singleton.getMainDomainBytes
    val otherDomainBytes = singleton.getOtherDomainBytes
    
    val acc = job.sparkContext.accumulator(0)
    
    val serviceOpsContent = task.getServiceOpsContents
    
    val outputRDD = inputRDD.map { pair => 
    
      val loader = LoaderSingleton.init(mainDomainBytes, otherDomainBytes, serviceOpsContent)
      
      val serviceOps = LoaderSingleton.getServiceOperations
      
      val key = pair._1
      
      val outputObjects = new ArrayList[GraphObject]()
      
      if(serviceOps.getUpgradeOptions != null) {
        
        val lines = VitalSignsBinaryFormat.decodeBlockStrings(pair._2, 0, pair._2.length)

        for(line <- lines) {
        
          outputObjects.add( UpgradeDowngradeProcedure.upgrade(serviceOps, line, loader) )
          
        }
        
      } else {
        
        for( newer <- VitalSignsBinaryFormat.decodeBlock(pair._2, 0, pair._2.length) ) {
          
          outputObjects.add( UpgradeDowngradeProcedure.downgrade(serviceOps, newer, loader) )
        }
        
      }
      
      acc.+=(1)
      
      //encoded
      (key, VitalSigns.get.encodeBlock(outputObjects))
      
    }
    
    if( job.isNamedRDDSupported() ) {
      
      job.namedRdds.update(task.getOutputDatasetName, outputRDD)
      
    } else {
      
      job.datasetsMap.put(task.getOutputDatasetName, outputRDD)
      
    }
    
    task.getParamsMap.put(DowngradeUpgradeProcedureTask.DATASET_STATS, "Processsed " + acc.value + " blocks")
    task.getParamsMap.put(task.getOutputDatasetName, outputRDD)
    
  }
  
  
}