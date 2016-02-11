package ai.vital.aspen.data.impl

import java.util.ArrayList

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD.rddToSequenceFileRDDFunctions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode

import ai.vital.aspen.analysis.training.ModelTrainingJob
import ai.vital.aspen.data.SegmentImportJob
import ai.vital.aspen.groovy.data.tasks.SaveDataSetTask
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.task.TaskImpl
import ai.vital.hadoop.writable.VitalBytesWritable
import ai.vital.sql.model.VitalSignsToSqlBridge

class SaveDataSetTaskImpl(job: AbstractJob, task: SaveDataSetTask) extends TaskImpl[SaveDataSetTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
  }

  def execute(): Unit = {
    
     val rdd = job.getDataset(task.datasetName);

     if(rdd == null) throw new RuntimeException("Dataset not found: " + task.datasetName)
     
     if(task.outputPath.startsWith("spark-segment:")) {
      
      val segmentID = task.outputPath.substring("spark-segment:".length())
       
       handleSparkSegmentExport(segmentID, rdd)
       
     } else {
       
    	 val hadoopOutput = rdd.map( pair =>
      	 (new Text(pair._1), new VitalBytesWritable(pair._2))
  		 )
    			 
       hadoopOutput.saveAsSequenceFile(task.outputPath)
    			 
    	 
     }
     
     task.getParamsMap.put(SaveDataSetTask.SAVE_DATASET_PREFIX + task.datasetName, java.lang.Boolean.TRUE);
    
  }
  
  def handleSparkSegmentExport(segmentID: String, rdd : RDD[(String, Array[Byte])]) : Unit = {
   
    var saveMode : SaveMode = null
    if(task.getParamsMap.containsKey(ModelTrainingJob.saveModeOption.getLongOpt)) {
      saveMode = task.getParamsMap.get(ModelTrainingJob.saveModeOption.getLongOpt).asInstanceOf[SaveMode]
    } else {
      saveMode = SaveMode.Append
    }
    
    val tableName = job.getSystemSegment().getSegmentTableName(segmentID)
  
    val hiveContext = job.getHiveContext()
    
    
    
    val initDF = hiveContext.table(tableName)
    
    if(initDF == null) throw new RuntimeException("DataFrame for table: " + tableName + " not found")
    
    //convert 
    val newDF = SegmentImportJob.convertBlockRDDToDataFrame(hiveContext, SegmentImportJob.customSchema, rdd)
    
    
    var outputDF : DataFrame = null
    
    if(saveMode == SaveMode.Overwrite) {
      
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
			
			outputDF = hiveContext.createDataFrame(outputRDD, SegmentImportJob.customSchema)
    			
    }
    
    
        //table name is escaped
    val tempTableName = "temp_" + System.currentTimeMillis(); 
    
    outputDF.registerTempTable(tempTableName)
    
    hiveContext.sql("INSERT OVERWRITE TABLE " + tableName + " SELECT * FROM " + tempTableName).collect();
    
    hiveContext.dropTempTable(tempTableName)
    
//    val writer = outputDF.write
//    
//    outputDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName)

  }
  
}
