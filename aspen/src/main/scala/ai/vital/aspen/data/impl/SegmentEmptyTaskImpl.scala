package ai.vital.aspen.data.impl

import ai.vital.aspen.groovy.data.tasks.SegmentEmptyTask
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.task.TaskImpl

class SegmentEmptyTaskImpl(job: AbstractJob, task: SegmentEmptyTask) extends TaskImpl[SegmentEmptyTask](job.sparkContext, task) {

    
  def checkDependencies(): Unit = {
    
  }
  
  def execute(): Unit = {
    
    val tableName = job.getSystemSegment().getSegmentTableName(task.segmentID)
    
    println("Table Name: " + tableName)
    
    val hiveContext = job.getHiveContext()
    
    hiveContext.sql("TRUNCATE TABLE " + tableName ).collect();
    
  }
  
}