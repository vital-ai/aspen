package ai.vital.aspen.data.impl

import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.data.DataProfileTask
import ai.vital.vitalsigns.binary.VitalSignsBinaryFormat
import scala.collection.JavaConversions._
import java.util.ArrayList
import java.util.Collection
import java.util.HashSet
import java.util.HashMap
import org.apache.hadoop.fs.Path
import org.mortbay.util.UrlEncoded
import java.net.URLEncoder
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.commons.io.IOUtils
import java.io.OutputStream
import au.com.bytecode.opencsv.CSVWriter
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets

class DataProfileTaskImpl(job: AbstractJob, task: DataProfileTask)  extends TaskImpl[DataProfileTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
    
  }

  def execute(): Unit = {

    var inputRDD = job.getDataset( task.getInputDatasetName )
    
    val propertiesFilter = task.getPropertiesFilter
    
    val sc = job.sparkContext
    
    val totalValues = sc.accumulator(0)
    val totalObjects = sc.accumulator(0)
    
    val outputPathBase = task.getOutputPath
    
    //output [String, Object]
    val outputRDD = inputRDD.flatMap { pair => 
    
        val gs = VitalSignsBinaryFormat.decodeBlock(pair._2, 0, pair._2.length)
    
        val l : ArrayList[(String, Object)] = new ArrayList[(String, Object)]()
        
        for( x <- gs ) {
          
          totalObjects.+=(1)
          
          for( p <- x.getPropertiesMap ) {
            
            val pURI = p._1
            
            if(propertiesFilter.size() == 0 || propertiesFilter.contains(pURI)) {
              
            	val rv = p._2.rawValue()

              
              if(rv.isInstanceOf[Collection[_]]) {
                
                val c = rv.asInstanceOf[Collection[_]]
                
                for( v <- c) {
                  l.add((pURI, v.asInstanceOf[Object]))
                }
                
              } else {
                l.add((pURI, rv.asInstanceOf[Object]))
                    
              }
              
            }
            
          }
          
        }   

        if(l.size() > 0) {
        	totalValues += l.size()
        }
        
        l
        
    }
    
    var grouped = outputRDD.groupBy { pair =>
      pair._1
    }
    
    var uniqueValsCounts = grouped.map { pair =>
      
      val histogram = new HashMap[Object, Integer]
      
      for( v <- pair._2 ) {
//        uniqueVals.add(v._2)
        
        var c = histogram.get(v._2)
        
        if(c == null) {
        	c = 1
        } else {
          c = c.intValue() + 1
        }
        
        histogram.put(v._2, c)
        
      }
      
      val seq = histogram.toSeq;
      val sorted = seq.sortWith{ (leftE,rightE) => 
        var c = leftE._2.compareTo(rightE._2)
        
        if(c != 0) {
          c > 0
        } else {
          val v1 = leftE._1
          val v2 = rightE._1
          if(v1.isInstanceOf[Number] && v2.isInstanceOf[Number]) {
            v1.asInstanceOf[Number].doubleValue() <= v2.asInstanceOf[Number].doubleValue()
          } else {
            v1.toString().compareTo(v2.toString()) <= 0
          }
        }
        
      }
      
      (pair._1, sorted)
      
    }
    
    val propsCount = sc.accumulator(0)
    val filesCount = sc.accumulator(0)
    
    uniqueValsCounts.foreach { pair =>
      
      val pURI = pair._1
      
      val outputPath = new Path(outputPathBase, URLEncoder.encode(pURI, "UTF-8") + ".csv")
      
      val vals = pair._2
      
      val fs = FileSystem.get(outputPath.toUri(), new Configuration())
      
      var outputStream : OutputStream = null;
      
      var writer : CSVWriter = null
      
      
      try {
        
    	    outputStream = fs.create(outputPath)
          
          writer = new CSVWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))
          
          for( v <- vals ) {
            
            val seq = Array(pURI, "" + v._1, "" + v._2);
            
            writer.writeNext( seq )
            
          }
          
    	    propsCount.+=(vals.size)
          filesCount.+=(1)
          
      } finally {
        IOUtils.closeQuietly(writer)
        IOUtils.closeQuietly(outputStream)
      }
      
    }
    
    
    
    
//    val propertiesoutputRDD.map({ pair => pair._1}).distinct().collect()
    
    var stats = "Total objects: " + totalObjects.value + "\nTotal values: " + totalValues.value +
    "\nOutput properties count: " + filesCount.value + "\nOutput records count: " + propsCount.value
    
    task.getParamsMap.put(DataProfileTask.DATASET_STATS, stats)
    
  }
  
}