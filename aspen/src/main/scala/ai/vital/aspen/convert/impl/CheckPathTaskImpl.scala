package ai.vital.aspen.convert.impl

import java.io.FileNotFoundException

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import ai.vital.aspen.groovy.convert.tasks.CheckPathTask
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.task.TaskImpl

class CheckPathTaskImpl(job: AbstractJob, task: CheckPathTask) extends TaskImpl[CheckPathTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {

  }

  def execute(): Unit = {

    var flag : java.lang.Boolean = null
    
    if(task.path.startsWith("name:")) {
      
      flag = handleRDDName(task.path.substring(5));
      
    } else if(task.path.startsWith("spark-segment:")) {
      
      flag = handleSparkSegment(task.path.substring("spark-segment:".length))
      
    } else {
      
      flag = handleFS();
      
    }
    
    task.getParamsMap.put(CheckPathTask.PATH_EXISTS_PREFIX + task.path, flag);
    
  }
  
  def handleSparkSegment(segmentID : String) : java.lang.Boolean = {
    
    return job.getSystemSegment().segmentExists(segmentID)
    
  }
  
  def handleRDDName(n : String) : java.lang.Boolean = {
    
//    if(task.acceptDirectories) ex("RDD are not directories, cannot accept path: " + task.path)
//      
//    if( ! job.isNamedRDDSupported() ) {
//      
//      throw new RuntimeException("NamedRDDs not supported")
//      
//    }
    
    
    var rdd = job.getDatasetOrNull(n); //job.namedRdds.get[(String, Array[Byte])](n)
    
    if(rdd == null) {
      
      if( task.mustExist ) ex("Required path (RDD) does not exist: " + n);
      
    } else {
      
      if(task.mustnotExist) ex("Path (RDD) already exists: " + n)
      
    }
    
    return rdd != null
    
//    var rdd = job.namedRdds.get[(String, Array[Byte])](n)
//    
//    if(!rdd.isDefined) {
//      
//      if( task.mustExist ) ex("Required path (RDD) does not exist: " + n);
//      
//    } else {
//      
//      if(task.mustnotExist) ex("Path (RDD) already exists: " + n)
//      
//    }
//    
//    return rdd.isDefined
    
  }
  
  def handleFS() : java.lang.Boolean = {
    
      val path = new Path( task.path )
      
      val fs = FileSystem.get(path.toUri(), job.hadoopConfiguration)
      
      var status : FileStatus = null 
      try {
        status = fs.getFileStatus(path)
      } catch {
        case ex: FileNotFoundException => {}
      }
      
      if(status == null) {
        
        if( task.mustExist ) ex("Required path does not exist: " + path)
        
      } else {
        
        if(task.mustnotExist) ex("Path already exists: " + path)
        
        if(status.isDirectory()) {
          
          if(!task.acceptDirectories) ex("Path is a directory: " + path + " - only files allowed")
          
          
          if(task.validFileExtensions != null) {
            
            validateFileExtensions(fs, status);
            
          }
          
          
        } else if(status.isFile()) {
          
          if(!task.acceptFiles) ex("Path is a file: " + path + " - only directories allowed")
          
          if(task.validFileExtensions != null) {
            
            validateFileExtensions(fs, status)
            
          }
          
          
        } else {
          ex("Path " + path + " is not a file nor a directory")
        }
        
        
      }
      
      var flag = new java.lang.Boolean(false)
      if(status != null) flag = true
      
      flag
      
  }
  
  def validateFileExtensions(fs: FileSystem, status: FileStatus) : Unit = {
    
    if(status.isDirectory()) {
      
      for( sub <- fs.listStatus(status.getPath) ) {

        if( task.singleDirectory ) {
          
        	if( sub.isDirectory() ) {
        	  
        	  if(sub.getPath.getName.equalsIgnoreCase("_temporary")) {
        	    
        	    //that's ok
        	    
        	  } else {
        	    
        		  ex("Nested directory found in path: " + status.getPath.toString() + " / " + sub.getPath.toString() + " - only single directory allowed")
        		  
        	  }
        	  
        		
          }
          
        } else {
          
        	validateFileExtensions(fs, sub)
        	
        }
        
        
      }
      
    } else {
      
      var valid = false
      for( ext <- task.validFileExtensions ) {
        
        if( status.getPath.getName.endsWith(ext) ) valid = true 
        
        
        
      }
      
      if( ! valid ) ex("File path " + status.getPath + " must end with: " + task.validFileExtensions.toList);
      
      
    }
    
  }
  
  
}
