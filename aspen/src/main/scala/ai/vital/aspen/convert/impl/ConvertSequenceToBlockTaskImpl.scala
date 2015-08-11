package ai.vital.aspen.convert.impl

import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.groovy.convert.tasks.ConvertSequenceToBlockTask
import org.apache.spark.SparkContext
import scala.collection.JavaConversions._
import org.apache.hadoop.fs.Path
import ai.vital.vitalsigns.block.BlockCompactStringSerializer
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.hadoop.fs.FileSystem
import ai.vital.aspen.job.AbstractJob
import java.util.ArrayList
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import org.apache.spark.rdd.RDD
import ai.vital.vitalsigns.VitalSigns
import ai.vital.hadoop.writable.VitalBytesWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.io.SequenceFile.CompressionType
import java.util.HashSet
import java.util.zip.GZIPInputStream
import java.nio.charset.StandardCharsets
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.BlockIterator
import java.io.OutputStreamWriter
import java.io.BufferedWriter
import ai.vital.aspen.data.LoaderSingleton
import ai.vital.vitalsigns.binary.VitalSignsBinaryFormat

class ConvertSequenceToBlockTaskImpl(job: AbstractJob, task: ConvertSequenceToBlockTask) extends TaskImpl[ConvertSequenceToBlockTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
    
    for(x <- task.inputPaths ) {

      if(x.startsWith("name:")) {
        
        if( ! job.isNamedRDDSupported() ) {
          throw new RuntimeException("NamedRDDs not supported")
        }     
        
        if( ! job.namedRdds.get(x.substring(5)).isDefined ) {
        	throw new RuntimeException("NamedRDD: " + x + " not found")
        }
        
      }
      
    }
  }

  def execute(): Unit = {
    
//    task.inputPaths
    if( task.outputPath .startsWith("name:") ) {
      throw new RuntimeException("Output must not be a namedRDD")
    }
    
    val outputPath = new Path(task.outputPath)
    
    val outputFS = FileSystem.get(outputPath.toUri(), job.hadoopConfiguration)
    
    val outputStream : java.io.OutputStream = outputFS.create(outputPath, true)
    
    val bw = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))
    
    val loader = LoaderSingleton.getActiveOutputLoader()
    
    var writer : BlockCompactStringSerializer = null;
    
    if(loader != null) {
      
      writer = new BlockCompactStringSerializer(bw, loader.getDomainURI2VersionMap)
      
    } else {
      
    	writer = new BlockCompactStringSerializer(bw)
    }
    
    
    var c = 0
    
    for(x <- task.inputPaths ) {

      var inputBlockRDD : RDD[(String, Array[Byte])] = null
      
      if(x.startsWith("name:")) {
      
        inputBlockRDD = job.namedRdds.get[(String, Array[Byte])](x.substring(5)).get
        
//        for( p <- inputBlockRDD.partitions ) {
//          
//          inputBlockRDD.iterator(split, context)
//          
//        }
        
        inputBlockRDD.foreachPartition { f =>
          
          for( g <- f) {
            
        	  writer.startBlock()
            
            if(loader != null) {
              
              for( s <- VitalSignsBinaryFormat.decodeBlockStrings(g._2, 0, g._2.length) ) {
                
                val go  = loader.readConverted(s)
                
                writer.writeGraphObject(go)
                
              }
              
            } else {
              
            	val b = VitalSigns.get.decodeBlock(g._2, 0, g._2.length)
            			
            	for(go <- b) {
            	  writer.writeGraphObject(go)
              }
            	
              
            }
            
        	  writer.endBlock()
            
            
          }
          
          c= c+1
          
        }
      } else {
        
    	  val inputPath = new Path(x)
    	  
        //don't convert it twice !
        
        val inputFS = FileSystem.get(inputPath.toUri(), job.hadoopConfiguration)
        
        val filesList = new ArrayList[Path]()
        
        if( inputFS.isDirectory(inputPath) ) {
          
          for( x <- inputFS.listStatus(inputPath) ) {
            
            if( x.isFile() && x.getPath.getName.startsWith("part-") ) {
              
              filesList.add(x.getPath)
              
            }
            
          }
          
        } else {
          
          filesList.add(inputPath)
          
        }
        
        
        for( path <- filesList ) {
          
        	val reader = new SequenceFile.Reader(inputFS, path, job.hadoopConfiguration)
        	
        	val key = new Text()
        	
        	val v = new VitalBytesWritable()
        	
        	while ( reader.next(key, v) ) {
        		
        		writer.startBlock()
            
            if(loader != null) {
              
              for( s <- VitalSignsBinaryFormat.decodeBlockStrings(v.get(), 0, v.get().length) ) {
                
                val go  = loader.readConverted(s)
                
                writer.writeGraphObject(go)
                
              }
              
            } else {
              
            	val block = VitalSigns.get().decodeBlock(v.get(), 0, v.get().length)
            			
              for (g <- block) {
            	  writer.writeGraphObject(g)
              }
              
            }
        		writer.endBlock()
        		
        		c= c+1
        		
        	}
        	
        	reader.close();
          
        }
        
      }
      
    }
    
    writer.flush()
    
    writer.close()
    
    task.getParamsMap.put(ConvertSequenceToBlockTask.VITAL_SEQUENCE_TO_BLOCK_PREFIX + task.outputPath, new java.lang.Integer(c));
    
    println("Output blocks count: " + c)
    
  }
  
}