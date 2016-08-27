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

class DatasetImportJob {}

object DatasetImportJob extends AbstractJob {
  
  val inputOption  = new Option("i", "input", true, "input .vital[.gz] block file")
  inputOption.setRequired(true)
  
  val nameOption   = new Option("n", "name", true, "output RDD[(String, Array[Byte]) name option, must start with 'name:' prefix")
  nameOption.setRequired(true)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite output if exists")
  overwriteOption.setRequired(false)
  
  def getJobClassName(): String = {
    classOf[DatasetImportJob].getCanonicalName
  }

  def getJobName(): String = {
    "dataset-import"
  }

  def getOptions(): Options = {
    addJobServerOptions(
      new Options().
        addOption(masterOption).
        addOption(inputOption).
        addOption(nameOption).
        addOption(profileOption).
        addOption(profileConfigOption).
        addOption(overwriteOption)
    )
  }

  def runJob(sc: SparkContext, jobConfig: Config): Any = {
    
    val globalContext = new SetOnceHashMap()
    
    val ic = jobConfig.getValue(inputOption.getLongOpt)
    
    var inputPaths : java.util.List[String] = null
    
    if(ic.isInstanceOf[ConfigList]) {
      
      throw new RuntimeException("single input allowed")
//      inputPaths = jobConfig.getStringList(inputOption.getLongOpt);
      
    } else {
      
      inputPaths = Arrays.asList(jobConfig.getString(inputOption.getLongOpt))
      
    }
    
    
    val outputName = jobConfig.getString(nameOption.getLongOpt)
    
    if( ! outputName.startsWith("name:")) {
      throw new RuntimeException("Output must with name: " + outputName)
    }
    
    println("Input: " + inputPaths)
    println("Output: " + outputName)
    
    val overwrite = getBooleanOption(jobConfig, overwriteOption)
    
    println("Overwrite ? " + overwrite)
    
    val datasetImportProcedure = new DatasetImportProcedure(inputPaths, outputName.substring(5), overwrite, globalContext)
 
    val tasks  = datasetImportProcedure.generateTasks();
    
    val handler = new TasksHandler()
    
    handler.handleTasksList(this, tasks)

    println("DONE")
    
  }
  
  override def subvalidate(sc : SparkContext, config : Config) : SparkJobValidation = {
 
    val outputName = config.getString(nameOption.getLongOpt)
    
    if( ! outputName.startsWith("name:")) {
      throw new RuntimeException("Output name must start with 'name:' " + outputName)
    }
    
    SparkJobValid
    
    
  }
  
  def main(args: Array[String]): Unit = {
    
    _mainImpl(args)
     
  }
  
  
}