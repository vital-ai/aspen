package ai.vital.aspen.convert.impl

import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.groovy.convert.tasks.ConvertSequenceToCsvTask
import ai.vital.aspen.task.TaskImpl
import scala.collection.JavaConversions._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Text
import ai.vital.hadoop.writable.VitalBytesWritable
import java.util.ArrayList
import ai.vital.vitalsigns.VitalSigns
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.compress.GzipCodec
import ai.vital.aspen.groovy.convert.tasks.ConvertCsvToSequenceTask

class ConvertSequenceToCsvTaskImpl(job: AbstractJob, task: ConvertSequenceToCsvTask) extends TaskImpl[ConvertSequenceToCsvTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
    
    for(x <- task.inputPaths ) {

      if(x.startsWith("name:")) {
        
        if( ! job.isNamedRDDSupported() ) {
          throw new RuntimeException("NamedRDDs not supported")
        }     
        
        if( ! job.namedRdds.get(x.substring(5)).isDefined ) {
        	throw new RuntimeException("NamedRDD: " + x + " not found")
        }
        
      } else if(x.startsWith("spark-segment:")) {
    	  throw new RuntimeException("Input must not be a namedRDD")
      }         
      
    }
    
    if( task.outputPath.startsWith("spark-segment:")) {
    	throw new RuntimeException("Output must not be a spark segment")
    }
    
    if( task.outputPath .startsWith("name:") ) {
      throw new RuntimeException("Output must not be a namedRDD")
    }
    
  }

  def execute(): Unit = {
    
//    task.inputPaths
    
    var inputBlockRDD : RDD[(String, Array[Byte])] = null
    
    for(inputPath <- task.inputPaths) {

      var blockRDD : RDD[(String, Array[Byte])] = null
      
      if(inputPath.startsWith("name:")) {
        
        blockRDD = job.namedRdds.get[(String, Array[Byte])](inputPath.substring("name:".length())).get
        
      } else {
        
    	  blockRDD = job.sparkContext.sequenceFile(inputPath, classOf[Text], classOf[VitalBytesWritable]).map { pair =>
        
      	  (pair._1.toString(), pair._2.get)
      	  
    	  }
    	  
      }
      
      if(inputBlockRDD == null) {
        
        inputBlockRDD = blockRDD
        
      } else {
        
        inputBlockRDD = inputBlockRDD.union(blockRDD)
        
      }

    }
    
    var csvRDD = inputBlockRDD.flatMap { encoded =>
    
        val l = new ArrayList[String]
      
        for( g <- VitalSigns.get().decodeBlock(encoded._2, 0, encoded._2.length) ) {
          
          l.addAll( g.toCSV(false) )
          
        }
        
        l
    }

    if(task.outputPath.endsWith(".gz")) {
      
    	csvRDD.saveAsTextFile(task.outputPath, classOf[GzipCodec])
    	
    } else {
      
    	csvRDD.saveAsTextFile(task.outputPath)
    	
    }
    
    task.getParamsMap.put(ConvertSequenceToCsvTask.VITAL_SEQUENCE_TO_CSV_PREFIX + task.outputPath, new java.lang.Boolean(true));
    
  }  
}