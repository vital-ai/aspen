package ai.vital.aspen.data.impl

import java.util.ArrayList

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode

import ai.vital.aspen.data.SegmentImportJob
import ai.vital.aspen.groovy.data.tasks.SegmentImportTask
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.task.TaskImpl
import ai.vital.sql.model.VitalSignsToSqlBridge

class SegmentImportTaskImpl(job: AbstractJob, task: SegmentImportTask) extends TaskImpl[SegmentImportTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
    
  }
  
  def execute(): Unit = {
    
    val tableName = job.getSystemSegment().getSegmentTableName(task.segmentID)
    
    println("Table Name: " + tableName)
    
    val hiveContext = job.getHiveContext()
    

    //obtain segment table name from vital-sql
    

    val initDF = hiveContext.table(tableName)
    
    var outputDF : DataFrame = null
    
    var inPath = "";
    
    for( path <- task.inputPaths ) {

      if(inPath.length() > 0 ) inPath += ","
      
      inPath += path;
        
    }

    val newDF = SegmentImportJob.readDataFrame(hiveContext, SegmentImportJob.customSchema, inPath)
    
    if(task.overwrite) {
      
      outputDF = newDF
    
      
    } else {
      
    	val g1 = initDF.map { r => ( r.getAs[String](VitalSignsToSqlBridge.COLUMN_URI), r) }.groupBy({ p => p._1})
			val g2 = newDF.map { r => ( r.getAs[String](VitalSignsToSqlBridge.COLUMN_URI), r ) }.groupBy({ p => p._1})
			
			val join = g1.fullOuterJoin(g2).map { pair =>
			
  			val oldProps = pair._2._1
			
	  		val newProps = pair._2._2
			
		  	if(newProps.size < 1) {
			  	oldProps.get
  			} else {
	  			newProps.get
		  	}
			
			}
			
			val outputRDD = join.flatMap { pair =>
			
			  val l = new ArrayList[Row]()
			
			  for( p <- pair ) {
				
				  l.add(p._2)
				
			  }
			
			  l
			
			}
    			
    	//very simple yet requires some driver memory to collect all uris in source segment
//    val newURIs = newDF.map { r => r.getAs[String](COLUMN_URI) }.distinct().collect()
//    val outputDF = initDF.filter(initDF.col(COLUMN_URI).isin(newURIs : _*).unary_!).unionAll(newDF)
    			
    	outputDF = hiveContext.createDataFrame(outputRDD, SegmentImportJob.customSchema)
    	
    }
    

    val saveMode = SaveMode.Overwrite
    
    outputDF.write.mode(saveMode).saveAsTable(tableName)

  }
  
}