package ai.vital.aspen.data

import java.util.ArrayList

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.spark.SparkContext

import com.typesafe.config.Config

import ai.vital.aspen.convert.impl.CheckPathTaskImpl
import ai.vital.aspen.convert.impl.DeletePathTaskImpl
import ai.vital.aspen.data.impl.ExportNamedRDDToSequenceTaskImpl
import ai.vital.aspen.groovy.convert.tasks.CheckPathTask
import ai.vital.aspen.groovy.convert.tasks.DeletePathTask
import ai.vital.aspen.groovy.task.AbstractTask
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.util.SetOnceHashMap
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobValidation

class ExportNamedRDDToSequence {
}

object ExportNamedRDDToSequence extends AbstractJob {
  
  val outputOption  = new Option("o", "output", true, "output <Text, VitalBytesWritable>  .vital.seq sequence path (directory)")
  outputOption.setRequired(true)
  
  val nameOption   = new Option("n", "name", true, "input RDD[(String, Array[Byte]) name option, must start with 'name:' prefix")
  nameOption.setRequired(true)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite output if exists")
  overwriteOption.setRequired(false)
  
  
  def getJobClassName(): String = {
    classOf[ExportNamedRDDToSequence].getCanonicalName
  }
  
  def getJobName(): String = {
		 "export-named-rdd-to-seq"
  }

  def getOptions(): Options = {
    addJobServerOptions(
      new Options().
        addOption(masterOption).
        addOption(outputOption).
        addOption(nameOption).
        addOption(overwriteOption)
    )
  }

  def runJob(sc: ExportNamedRDDToSequence.C, jobConfig: Config): Any = {
    
  val globalContext = new SetOnceHashMap()
    
    val inputPath = jobConfig.getString(nameOption.getLongOpt)
    
    val outputPath = jobConfig.getString(nameOption.getLongOpt)
    
    if( ! inputPath.startsWith("name:")) {
      throw new RuntimeException("Input must start with name: " + inputPath)
    }
    
    val overwrite = getBooleanOption(jobConfig, overwriteOption)
    
    val tasks  = new ArrayList[AbstractTask]
    
    
    val cpt = new CheckPathTask(inputPath, globalContext)
    cpt.acceptDirectories = true
    cpt.acceptFiles = true
    cpt.mustExist = true
      
    tasks.add(cpt)
    
    if(overwrite) {
      tasks.add(new DeletePathTask(outputPath, globalContext))
    }
    
    tasks.add(new ExportNamedRDDToSequenceTask(inputPath.substring(5), outputPath, globalContext))
    
    val totalTasks = tasks.size()
    
    var currentTask = 0
    
    for( task <- tasks ) {
      
      currentTask = currentTask + 1
      
      println ( "Executing task: " + task.getClass.getCanonicalName + " [" + currentTask + " of " + totalTasks + "]")
      
      for(i <- task.getRequiredParams) {
        if(!globalContext.containsKey(i)) throw new RuntimeException("Task " + task.getClass.getSimpleName + " input param not set: " + i)
      }
      
      //any inner dependencies
      task.checkDepenedencies()
      
      var taskImpl : TaskImpl[_] = null
      
      if(task.isInstanceOf[CheckPathTask]) {
        
        taskImpl = new CheckPathTaskImpl(this, task.asInstanceOf[CheckPathTask])
        
      } else if(task.isInstanceOf[DeletePathTask]) {
        
        taskImpl = new DeletePathTaskImpl(this, task.asInstanceOf[DeletePathTask])
        
      } else if(task.isInstanceOf[ExportNamedRDDToSequenceTask]) {
        
        taskImpl = new ExportNamedRDDToSequenceTaskImpl(this, task.asInstanceOf[ExportNamedRDDToSequenceTask])
        
      } else {
        throw new RuntimeException("Unhandled task: " + task.getClass.getCanonicalName);
      }
      
      if(taskImpl != null) {
        
        taskImpl.checkDependencies()
        
        taskImpl.execute()
        
      }

      for(x <- task.getOutputParams) {
        if(!globalContext.containsKey(x)) throw new RuntimeException("Task " + task.getClass.getCanonicalName + " did not return param: " + x);
      }
      
      
      //inner validation
      task.onTaskComplete()
      
    }
      
    println("DONE")
  }
  
  override def subvalidate(sc : SparkContext, config : Config) : SparkJobValidation = {
 
    val inputName = config.getString(nameOption.getLongOpt)
    
    if( ! inputName.startsWith("name:")) {
      throw new RuntimeException("Output must with name: " + inputName)
    }
    
    SparkJobValid
    
  }
  
  def main(args: Array[String]): Unit = {
    
    _mainImpl(args)
     
  }
  
  
  
}