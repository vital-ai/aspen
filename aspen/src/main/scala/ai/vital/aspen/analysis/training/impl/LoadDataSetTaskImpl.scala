package ai.vital.aspen.analysis.training.impl

import ai.vital.aspen.analysis.training.AbstractModelTrainingTaskImpl
import org.apache.spark.SparkContext
import ai.vital.aspen.groovy.predict.tasks.LoadDataSetTask
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

class LoadDataSetTaskImpl(sc: SparkContext, task: LoadDataSetTask, setServiceProfile: String) extends AbstractModelTrainingTaskImpl[LoadDataSetTask](sc, task) {
  
  def checkDependencies(): Unit = {
    if(ModelTrainingJob.isNamedRDDSupported()) {
//      if( this.namedRdds.get(ldt.datasetName).isDefined ) throw new RuntimeException("Dataset already loaded: " + ldt.datasetName)
    } else {
//      if( datasetsMap.get(ldt.datasetName) != null) throw new RuntimeException("Dataset already loaded: " + ldt.datasetName);
    }
  }

  def execute(): Unit = {
    
    val inputName = task.path
    
    var inputBlockRDD : RDD[(String, Array[Byte])] = null
    
    var inputRDDName : String = null
    if(inputName.startsWith("name:")) {
      inputRDDName = inputName.substring("name:".length())
    }

    val model = task.getModel
    
    val serviceProfile = setServiceProfile
    
    if(inputRDDName == null) {
        
      println("input path: " + inputName)
        
      val inputPath = new Path(inputName)
        
      val inputFS = FileSystem.get(inputPath.toUri(), ModelTrainingJob.hadoopConfig)
        
      if (!inputFS.exists(inputPath) /*|| !inputFS.isDirectory(inputPath)*/) {
        throw new RuntimeException("Input train path does not exist " + /*or is not a directory*/ ": " + inputPath.toString())
      }
        
      val inputFileStatus = inputFS.getFileStatus(inputPath)
        
      if(inputName.endsWith(".vital") || inputName.endsWith(".vital.gz")) {
            
        if(!inputFileStatus.isFile()) {
          throw new RuntimeException("input path indicates a block file but does not denote a file: " + inputName)
        }
        throw new RuntimeException("Vital block files not supported yet")
            
      } else {
          
        inputBlockRDD = sc.sequenceFile(inputPath.toString(), classOf[Text], classOf[VitalBytesWritable]).map { pair =>
            
          //make sure the URIReferences are resolved
          val inputObjects = VitalSigns.get().decodeBlock(pair._2.get, 0, pair._2.get.length)
          val vitalBlock = new VitalBlock(inputObjects)
            
          if(vitalBlock.getMainObject.isInstanceOf[URIReference]) {
              
            if( VitalSigns.get.getVitalService == null ) {
              if(serviceProfile != null) VitalServiceFactory.setServiceProfile(serviceProfile)
              VitalSigns.get.setVitalService(VitalServiceFactory.getVitalService)
            }
              
            //loads objects from features queries and train queries 
            model.getFeatureExtraction.composeBlock(vitalBlock)
              
            (pair._1.toString(), VitalSigns.get.encodeBlock(vitalBlock.toList()))
              
          } else {
              
            (pair._1.toString(), pair._2.get)
          }

            
        }
         
        inputBlockRDD.cache()
          
      }
        
    } else {
      
      inputBlockRDD = ModelTrainingJob.namedRdds.get[(String, Array[Byte])](inputRDDName).get
      
      //quick scan to make sure the blocks are fetched
      inputBlockRDD = inputBlockRDD.map { pair =>
        
        
      //make sure the URIReferences are resolved
      val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
      val vitalBlock = new VitalBlock(inputObjects)
            
      if(vitalBlock.getMainObject.isInstanceOf[URIReference]) {
          
        if( VitalSigns.get.getVitalService == null ) {
          if(serviceProfile != null) VitalServiceFactory.setServiceProfile(serviceProfile)
          VitalSigns.get.setVitalService(VitalServiceFactory.getVitalService)
        }
              
        //loads objects from features queries and train queries 
        model.getFeatureExtraction.composeBlock(vitalBlock)
              
        (pair._1.toString(), VitalSigns.get.encodeBlock(vitalBlock.toList()))
              
      } else {
              
        pair
              
      }
        
    }
      
  }
    
  if(ModelTrainingJob.isNamedRDDSupported()) {
      ModelTrainingJob.namedRdds.update(task.datasetName, inputBlockRDD);
    } else {
      ModelTrainingJob.datasetsMap.put(task.datasetName, inputBlockRDD)
    }
    
    ModelTrainingJob.globalContext.put(task.datasetName, inputBlockRDD)
  }
  
}