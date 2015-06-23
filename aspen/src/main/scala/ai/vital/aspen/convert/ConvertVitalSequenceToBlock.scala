package ai.vital.aspen.convert

import scala.collection.JavaConversions.asScalaBuffer
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import ai.vital.aspen.groovy.convert.ConvertSequenceToBlockProcedure
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.util.SetOnceHashMap
import ai.vital.aspen.groovy.convert.tasks.CheckPathTask
import ai.vital.aspen.convert.impl.CheckPathTaskImpl
import org.apache.hadoop.conf.Configuration
import ai.vital.aspen.groovy.convert.tasks.ConvertSequenceToBlockTask
import ai.vital.aspen.convert.impl.ConvertSequenceToBlockTaskImpl
import ai.vital.aspen.groovy.convert.tasks.DeletePathTask
import ai.vital.aspen.convert.impl.DeletePathTaskImpl
import com.typesafe.config.ConfigList
import java.util.Arrays
import ai.vital.aspen.groovy.convert.ConvertSequenceToBlockProcedure

class ConvertVitalSequenceToBlock {}

object ConvertVitalSequenceToBlock extends AbstractJob {
  
  val inputOption  = new Option("i", "input", true, "input <Text, VitalBytesWritable>  .vital.seq sequence file or <String, Array[Byte]> named RDD, if path starts with 'name:' prefix")
  inputOption.setRequired(true)

  val outputOption = new Option("o", "output", true, "output .vital[.gz] file")
  outputOption.setRequired(true)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite output if exists")
  overwriteOption.setRequired(false)

  def getJobClassName(): String = {
    classOf[ConvertVitalBlockToSequence].getCanonicalName    
  }

  def getJobName(): String = {
    "convert-block-to-seq"
  }

  def getOptions(): Options = {
    addJobServerOptions(
      new Options()
        .addOption(masterOption)
        .addOption(inputOption)
        .addOption(outputOption)
        .addOption(overwriteOption)
    )
  }

  def runJob(sc: SparkContext, jobConfig: Config): Any = {

    val globalContext = new SetOnceHashMap()
    
    val ic = jobConfig.getValue(inputOption.getLongOpt)
    
    var inputPaths : java.util.List[String] = null
    
    if(ic.isInstanceOf[ConfigList]) {
      
    	inputPaths = jobConfig.getStringList(inputOption.getLongOpt);
      
    } else {
      
      inputPaths = Arrays.asList(jobConfig.getString(inputOption.getLongOpt))
      
    }
    
    
    val outputPath = jobConfig.getString(outputOption.getLongOpt)
    
    val overwrite = getBooleanOption(jobConfig, overwriteOption)
    
    val procedure = new ConvertSequenceToBlockProcedure(inputPaths, outputPath, overwrite, globalContext)
    
    val tasks = procedure.generateTasks()
    
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
        
      } else if(task.isInstanceOf[ConvertSequenceToBlockTask]) {
        
        taskImpl = new ConvertSequenceToBlockTaskImpl(this, task.asInstanceOf[ConvertSequenceToBlockTask]) 
        
      } else if(task.isInstanceOf[DeletePathTask]) {
        
        taskImpl = new DeletePathTaskImpl(this, task.asInstanceOf[DeletePathTask])
        
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
      
    
  }
  

  def main(args: Array[String]): Unit = {
    
    _mainImpl(args)
     
  }
  
}
