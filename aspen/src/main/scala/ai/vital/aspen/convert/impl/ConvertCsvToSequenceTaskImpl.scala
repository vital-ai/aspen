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
import ai.vital.sql.model.VitalSignsToSqlBridge
import org.apache.commons.csv.CSVFormat
import java.io.StringReader
import ai.vital.sql.services.ToCSVProviderImpl
import ai.vital.aspen.data.SegmentImportJob
import ai.vital.aspen.groovy.convert.tasks.ConvertCsvToSequenceTask

class ConvertCsvToSequenceTaskImpl(job: AbstractJob, task: ConvertCsvToSequenceTask) extends TaskImpl[ConvertCsvToSequenceTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
    
    for(x <- task.inputPaths ) {

      if(x.startsWith("name:")) {
        
        throw new RuntimeException("Input must not be a namedRDD")
        
      } else if(x.startsWith("spark-segment:")) {
        
        throw new RuntimeException("Spark segment input not supported")
        
      }
      
    }
    
    if( task.outputPath.startsWith("spark-segment:")) {
    	throw new RuntimeException("Output must not be a spark segment")
    }
    
  }

  def execute(): Unit = {
    
//    task.inputPaths
    
    var inputBlockRDD : RDD[(String, Array[Byte])] = null
    
    val headers = new ToCSVProviderImpl().getHeaders;
    
    val uriIndex = headers.indexOf(VitalSignsToSqlBridge.COLUMN_URI)
    
    for(inputPath <- task.inputPaths) {

      println ("Reading in path: "  + inputPath)
      
      //map that into 
      val blockRDD = job.sparkContext.textFile(inputPath).flatMap { x => 
      
        val l = new ArrayList[(String, List[String])]
        
        if(x.isEmpty()) {
          
        } else {
          
        	//read csv file and group it by uri
        	val parser = CSVFormat.DEFAULT.parse(new StringReader(x))
        	
        	val records = parser.getRecords
        			
        	for ( r <- records ) {
        				
        	  val uri = r.get(uriIndex)
        	  
        	  val rec = new ArrayList[String]()
        	  
        	  for ( v <- r ) {
        	    rec.add(v)
        	  }
        	  
        	  l.add( ( uri, r.toList ) )
        	  
          }
        	
        	parser.close()
        	
        }
        
        l
        
//        VitalSignsToSqlBridge.fromSql(x$1, x$2, x$3, x$4, x$5)
        
      }.groupByKey().map { uri2records =>
        
        val rows = new ArrayList[java.util.Map[String,Object]]() 
        
        for( r <- uri2records._2 ) {
          
          val m = SegmentImportJob.csvRecordToRowMap(r)
          
          rows.add(m)
          
        }
        
        val results = VitalSignsToSqlBridge.fromSql(null, null, rows, null, null)
        
        (uri2records._1, VitalSigns.get.encodeBlock(results) )
        
      }
      
      if( inputBlockRDD == null ) {
        
        inputBlockRDD = blockRDD
        
      } else {
        
        inputBlockRDD = inputBlockRDD.union(blockRDD)
        
      }
      
    }
    
    if(task.outputPath.startsWith("name:")) {
      
      val outputRDDName = task.outputPath.substring("name:".length)
      
      if(job.isNamedRDDSupported()) {
    	  job.namedRdds.update(outputRDDName, inputBlockRDD)
      } else {
        job.datasetsMap.put(outputRDDName, inputBlockRDD)
      }
      
    } else {
      
    	val hadoopOutput = inputBlockRDD.map( pair =>
    	  (new Text(pair._1), new VitalBytesWritable(pair._2))
      )
    			
    	hadoopOutput.saveAsSequenceFile(task.outputPath)
      
    } 
    
    task.getParamsMap.put(ConvertCsvToSequenceTask.VITAL_CSV_TO_SEQUENCE_PREFIX + task.outputPath, new java.lang.Boolean(true));
    
  }  
}