package ai.vital.aspen.data.impl

import ai.vital.aspen.groovy.data.tasks.SegmentExportTask
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.task.TaskImpl

class SegmentExportTaskImpl(job: AbstractJob, task: SegmentExportTask) extends TaskImpl[SegmentExportTask](job.sparkContext, task) {

    
  def checkDependencies(): Unit = {
    
  }
  
  def execute(): Unit = {
    
    val tableName = job.getSystemSegment().getSegmentTableName(task.segmentID)
    
    println("Table Name: " + tableName)
    
    val hiveContext = job.getHiveContext()
    
    //obtain segment table name from vital-sql

    val initDF = hiveContext.table(tableName)
    
    val writer = initDF.write
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("nullValue", "")
//    .option("treatEmptyValuesAsNulls", "true")
////    .schema(SegmentImportJob.customSchema)
//    .option("inferSchema", "false") // Automatically infer data types
//    .save(path);
  
    
    if(task.outputPath.endsWith(".gz")) {
      writer.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
    }
    
    writer.save(task.outputPath)

  }
  
}