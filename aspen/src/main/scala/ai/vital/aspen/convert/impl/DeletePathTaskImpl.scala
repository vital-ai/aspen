package ai.vital.aspen.convert.impl

import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.groovy.convert.tasks.DeletePathTask
import ai.vital.aspen.job.AbstractJob
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

class DeletePathTaskImpl(job: AbstractJob, task: DeletePathTask) extends TaskImpl[DeletePathTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
  }

  def execute(): Unit = {
    
    var flag : java.lang.Boolean = null
    
    if(task.path.startsWith("name:")) {
      
      flag = handleRDDName(task.path.substring(5));
      
    } else {
      
      flag = handleFS();
      
    }
    
    task.getParamsMap.put(DeletePathTask.PATH_DELETED_PREFIX + task.path, flag);
  }
  
  def handleRDDName(name: String) : java.lang.Boolean = {
  
    if( job.isNamedRDDSupported() ) {
    
      var rdd = job.namedRdds.get[(String, Array[Byte])](name)
      
      if(!rdd.isDefined) {
        return false
      }
       
      job.namedRdds.destroy(name)
      
    } else {
      
      return job.datasetsMap.remove(name) != null
      
    }   
    return true
    
  }
  
  def handleFS() : java.lang.Boolean = {
    
    val path = new Path(task.path)
    
    val fs = FileSystem.get(path.toUri(), job.hadoopConfiguration)
    
    val r = fs.delete(path, true)
    
    return r
    
  }
}