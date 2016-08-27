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
    
    val singleton = LoaderSingleton.getParent
    
    val mainDomainBytes = singleton.getMainDomainBytes
    val otherDomainBytes = singleton.getOtherDomainBytes
    
    val acc = job.sparkContext.accumulator(0)
    
    val serviceOpsContent = task.getServiceOpsContents
    
    val outputRDD = inputRDD.map { pair => 
    
      val child = LoaderSingleton.getChild(mainDomainBytes, otherDomainBytes, serviceOpsContent)
      
      
      val serviceOps = child.getServiceOps
      
      
      val loader = child.getLoader
      
      val key = pair._1
      
      val outputObjects = new ArrayList[GraphObject]()
      
      if(serviceOps.getUpgradeOptions != null) {
        
        val gs = VitalSignsBinaryFormat.decodeBlock(pair._2, 0, pair._2.length)

        for(g <- gs) {
        
          outputObjects.add( UpgradeDowngradeProcedure.upgrade(serviceOps, g, loader) )
          
        }
        
      } else {
        
        for( newer <- VitalSignsBinaryFormat.decodeBlock(pair._2, 0, pair._2.length) ) {
          
          outputObjects.add( UpgradeDowngradeProcedure.downgrade(serviceOps, newer, loader) )
        }
        
      }
      
      acc.+=(1)
      
      
      //only if downgrading!
      var encoded : Array[Byte] = null;
      
      if(serviceOps.getUpgradeOptions != null) {
        encoded = VitalSignsBinaryFormat.encodeBlock(outputObjects)
      } else {
        encoded = VitalSignsBinaryFormat.encodeBlock(outputObjects, loader.getDomainURI2VersionMap)
      }
      
      
      //encoded
      (key, encoded)
      
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