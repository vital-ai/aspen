package ai.vital.aspen.data

import ai.vital.aspen.job.AbstractJob
import org.apache.commons.cli.Options
import org.apache.commons.cli.Option
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import ai.vital.aspen.util.SetOnceHashMap
import com.typesafe.config.ConfigList
import java.util.Arrays
import ai.vital.aspen.groovy.task.AbstractTask
import scala.collection.JavaConversions._
import ai.vital.aspen.task.TaskImpl
import java.util.ArrayList
import ai.vital.aspen.groovy.convert.tasks.CheckPathTask
import ai.vital.aspen.groovy.convert.tasks.DeletePathTask
import spark.jobserver.SparkJobValidation
import spark.jobserver.SparkJobValid
import ai.vital.aspen.groovy.data.tasks.LoadDataSetTask
import ai.vital.aspen.convert.impl.CheckPathTaskImpl
import ai.vital.aspen.convert.impl.DeletePathTaskImpl
import ai.vital.aspen.data.impl.LoadDataSetTaskImpl
import ai.vital.aspen.groovy.data.DatasetImportProcedure
import ai.vital.aspen.job.TasksHandler
import ai.vital.aspen.groovy.data.DatasetExportProcedure

class DatasetExportJob {}

object DatasetExportJob extends AbstractJob {
  
  val outputOption  = new Option("o", "output", true, "output .vital[.gz] block file")
  outputOption.setRequired(true)
  
  val nameOption   = new Option("n", "name", true, "input RDD[(String, Array[Byte]) name option, must start with 'name:' prefix")
  nameOption.setRequired(true)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite output if exists")
  overwriteOption.setRequired(false)
  
  def getJobClassName(): String = {
    classOf[DatasetExportJob].getCanonicalName
  }

  def getJobName(): String = {
    "dataset-export"
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

  def runJob(sc: SparkContext, jobConfig: Config): Any = {
    
    val globalContext = new SetOnceHashMap()
    
    val outputPath = jobConfig.getString(outputOption.getLongOpt)
    
    val inputName = jobConfig.getString(nameOption.getLongOpt)
    
    if( ! inputName.startsWith("name:")) {
      throw new RuntimeException("Input must with name: " + inputName)
    }
    
    println("Input: " + inputName)
    println("Output: " + outputPath)
    
    val overwrite = getBooleanOption(jobConfig, overwriteOption)
    
    println("Overwrite ? " + overwrite)
    
    val datasetExportProcedure = new DatasetExportProcedure(inputName.substring(5), outputPath, overwrite, globalContext)
    
    val tasks  = datasetExportProcedure.generateTasks();
    
    val handler = new TasksHandler()
    
    handler.handleTasksList(this, tasks)

    println("DONE")
    
  }
  
  override def subvalidate(sc : SparkContext, config : Config) : SparkJobValidation = {
 
    val inputName = config.getString(nameOption.getLongOpt)
    
    if( ! inputName.startsWith("name:")) {
      throw new RuntimeException("Input name must start with 'name:' " + inputName)
    }
    
    SparkJobValid
    
    
  }
  
  def main(args: Array[String]): Unit = {
    
    _mainImpl(args)
     
  }
  
  
}
