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
import ai.vital.aspen.task.TaskImpl

class LoadDataSetTaskImpl(job: AbstractJob, task: LoadDataSetTask) extends TaskImpl[LoadDataSetTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
  }

  def execute(): Unit = {
    
    val inputName = task.path
    
    var inputBlockRDD : RDD[(String, Array[Byte])] = null
    
    var inputRDDName : String = null
    
    if(inputName.startsWith("name:")) {
      
      inputBlockRDD = job.getDataset(inputName.substring(5))
      try {
      task.getParamsMap.put( task.datasetName, inputBlockRDD )
      } catch {
        case ex: Exception => {}
      }
      return
      
    }
        
    println("input path: " + inputName)
        
    val inputPath = new Path(inputName)
        
    val inputFS = FileSystem.get(inputPath.toUri(), job.hadoopConfiguration)
        
    if (!inputFS.exists(inputPath) /*|| !inputFS.isDirectory(inputPath)*/) {
      throw new RuntimeException("Input train path does not exist " + /*or is not a directory*/ ": " + inputPath.toString())
    }
        
    val inputFileStatus = inputFS.getFileStatus(inputPath)
        
    inputBlockRDD = job.sparkContext.sequenceFile(inputPath.toString(), classOf[Text], classOf[VitalBytesWritable]).map { pair =>
            
      //make sure the URIReferences are resolved
      val inputObjects = VitalSigns.get().decodeBlock(pair._2.get, 0, pair._2.get.length)
      val vitalBlock = new VitalBlock(inputObjects)
            
      (pair._1.toString(), pair._2.get)
    }
    
    if(job.isNamedRDDSupported()) {
    	job.namedRdds.update(task.datasetName, inputBlockRDD);
    } else {
    	job.datasetsMap.put(task.datasetName, inputBlockRDD)
    }
    
    task.getParamsMap.put( task.datasetName, inputBlockRDD )
    
  }
  
}
