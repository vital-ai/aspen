package ai.vital.aspen.data.impl

import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.groovy.data.tasks.SegmentExportTask
import ai.vital.aspen.task.TaskImpl
import ai.vital.vitalsigns.VitalSigns
import ai.vital.sql.service.VitalServiceSql
import ai.vital.aspen.data.SegmentImportJob

class SegmentExportTaskImpl(job: AbstractJob, task: SegmentExportTask) extends TaskImpl[SegmentExportTask](job.sparkContext, task) {

    
  def checkDependencies(): Unit = {
    
  }
  
  def execute(): Unit = {
    
    val vitalService = VitalSigns.get.getVitalService
    
    if(vitalService == null) throw new RuntimeException("No vitalservice instance set in VitalSigns")
    
    if(!vitalService.isInstanceOf[VitalServiceSql]) throw new RuntimeException("Expected instance of " + classOf[VitalServiceSql].getCanonicalName)
    
    val vitalServiceSql = vitalService.asInstanceOf[VitalServiceSql]
    
    val segment = vitalServiceSql.getSegment(task.segmentID)
    
    if(segment == null) throw new RuntimeException("Segment not found: " + segment)

    val tableName = vitalService.asInstanceOf[VitalServiceSql].getSegmentTableName(segment)
    
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