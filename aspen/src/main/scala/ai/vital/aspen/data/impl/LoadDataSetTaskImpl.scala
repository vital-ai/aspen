package ai.vital.aspen.data.impl

import java.util.ArrayList
import java.util.HashMap

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.DataFrame

import ai.vital.aspen.data.LoaderSingleton
import ai.vital.aspen.groovy.data.tasks.LoadDataSetTask
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.task.TaskImpl
import ai.vital.hadoop.writable.VitalBytesWritable
import ai.vital.sql.model.VitalSignsToSqlBridge
import ai.vital.vitalsigns.VitalSigns

object LoadDataSetTaskImpl {
  
    def dataFrameToVitalBlockRDD(df : DataFrame) : RDD[(String, Array[Byte])] = {
    
    val grouped = df.map { row =>
      
      val uri = row.getAs[String](VitalSignsToSqlBridge.COLUMN_URI)
      
      (uri, row)
      
    }.groupByKey()
    
    val blockRDD : RDD[(String, Array[Byte])] = grouped.map { group =>
      
      val rowsList = new ArrayList[java.util.Map[String, Object]]()
      
      for(r <- group._2.seq ) {
        
        var i = 0;
        
        val row = new HashMap[String, Object]()
        
        while( i < r.size ) {

        	val f = r.schema.fields(i)
        	
          val v = r.get(i);
        	
        	if(v != null ) {
        		row.put(f.name, v.asInstanceOf[Object])
        	}
          
          i = i + 1
          
        }
        
        rowsList.add(row)
        
      }
      
      val results = VitalSignsToSqlBridge.fromSql(null, null, rowsList, null, null)
    	  
      (group._1, VitalSigns.get.encodeBlock(results))
      
    }
    
    blockRDD
    
  }
  
}

class LoadDataSetTaskImpl(job: AbstractJob, task: LoadDataSetTask) extends TaskImpl[LoadDataSetTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
  }

  def execute(): Unit = {
    
    val inputName = task.path
    
    var inputBlockRDD : RDD[(String, Array[Byte])] = null
    
    var inputRDDName : String = null
    
    if(inputName.startsWith("name:")) {
      
      inputBlockRDD = job.getDataset(inputName.substring(5))
      
      try {
        task.getParamsMap.put( task.datasetName, inputBlockRDD )
      } catch {
        case ex: Exception => {}
      }
      return
      
    } else if(inputName.startsWith("spark-segment:")) {
      
      val segmentID = inputName.substring("spark-segment:".length())
      
      inputBlockRDD = handleSparkSegment(segmentID)
      
      try {
        task.getParamsMap.put(task.datasetName, inputBlockRDD)
      } catch {
        case ex: Exception => {}
      }
      
      if(job.isNamedRDDSupported()) {
    	  job.namedRdds.update(task.datasetName, inputBlockRDD);
      } else {
    	  job.datasetsMap.put(task.datasetName, inputBlockRDD)
      }
      
      return
      
    }
        
    println("input path: " + inputName)
        
    val inputPath = new Path(inputName)
        
    val inputFS = FileSystem.get(inputPath.toUri(), job.hadoopConfiguration)
        
    if (!inputFS.exists(inputPath) /*|| !inputFS.isDirectory(inputPath)*/) {
      throw new RuntimeException("Input train path does not exist " + /*or is not a directory*/ ": " + inputPath.toString())
    }
        
    val inputFileStatus = inputFS.getFileStatus(inputPath)
    
    val loader = LoaderSingleton.getActiveInputLoader 
    
    val useSpecialLoader = (loader != null)
    
    
    inputBlockRDD = job.sparkContext.sequenceFile(inputPath.toString(), classOf[Text], classOf[VitalBytesWritable]).map { pair =>
      
//      if(useSpecialLoader)
      //make sure the URIReferences are resolved
//      val inputObjects = VitalSigns.get().decodeBlock(pair._2.get, 0, pair._2.get.length)
//      val vitalBlock = new VitalBlock(inputObjects)
            
      (pair._1.toString(), pair._2.get)
    }
    
    if(job.isNamedRDDSupported()) {
    	job.namedRdds.update(task.datasetName, inputBlockRDD);
    } else {
    	job.datasetsMap.put(task.datasetName, inputBlockRDD)
    }
    
    task.getParamsMap.put( task.datasetName, inputBlockRDD )
    
  }
  
  def handleSparkSegment(segmentID : String) : RDD[(String, Array[Byte])] = {

    val tableName = job.getSystemSegment().getSegmentTableName(segmentID)
    
    val hiveContext = job.getHiveContext()
    
    val df = hiveContext.table(tableName)

    if(df == null) throw new RuntimeException("DataFrame for table: " + tableName + " not found")
    
    return LoadDataSetTaskImpl.dataFrameToVitalBlockRDD(df)
    
  }
  
  
}
