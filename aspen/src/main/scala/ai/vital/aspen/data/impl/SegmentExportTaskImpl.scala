package ai.vital.aspen.data.impl

import ai.vital.aspen.groovy.data.tasks.SegmentExportTask
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.task.TaskImpl
import org.apache.hadoop.io.Text
import ai.vital.hadoop.writable.VitalBytesWritable

class SegmentExportTaskImpl(job: AbstractJob, task: SegmentExportTask) extends TaskImpl[SegmentExportTask](job.sparkContext, task) {

    
  def checkDependencies(): Unit = {
    
  }
  
  def execute(): Unit = {
    
    val tableName = job.getSystemSegment().getSegmentTableName(task.segmentID)
    
    println("Table Name: " + tableName)
    
    val hiveContext = job.getHiveContext()
    
    //obtain segment table name from vital-sql

    val initDF = hiveContext.table(tableName)
    
    if(task.outputPath.endsWith(".vital.seq")) {
      
      val blockRDD = LoadDataSetTaskImpl.dataFrameToVitalBlockRDD(initDF)
      
      val hadoopOutput = blockRDD.map( pair =>
      	 (new Text(pair._1), new VitalBytesWritable(pair._2))
  		 )
    			 
       hadoopOutput.saveAsSequenceFile(task.outputPath)
      
    } else {
      
    	val writer = initDF.write
    			.format("com.databricks.spark.csv")
    			.option("header", "true") // Use first line of all files as header
    			.option("nullValue", "")
    			
			if(task.outputPath.endsWith(".gz")) {
				writer.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
			}
    	
    	writer.save(task.outputPath)
    	
    }
    

  }
  
}