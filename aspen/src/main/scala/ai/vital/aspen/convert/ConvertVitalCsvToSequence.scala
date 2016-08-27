package ai.vital.aspen.convert

import scala.collection.JavaConversions.asScalaBuffer
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.util.SetOnceHashMap
import ai.vital.aspen.groovy.convert.tasks.CheckPathTask
import ai.vital.aspen.convert.impl.CheckPathTaskImpl
import org.apache.hadoop.conf.Configuration
import ai.vital.aspen.groovy.convert.tasks.DeletePathTask
import ai.vital.aspen.convert.impl.DeletePathTaskImpl
import com.typesafe.config.ConfigList
import java.util.Arrays
import ai.vital.aspen.job.TasksHandler
import ai.vital.aspen.groovy.convert.ConvertCsvToSequenceProcedure

class ConvertVitalCsvToSequence {}

object ConvertVitalCsvToSequence extends AbstractJob {
  
  val inputOption  = new Option("i", "input", true, "input .vital.csv[.gz] file(s), if a directory all files must be vital csv gz files, output will be merged")
  inputOption.setRequired(true)

  val outputOption = new Option("o", "output", true, "output <Text, VitalBytesWritable> .vital.seq sequence file or <String, Array[Byte]> named RDD, if path starts with 'name:' prefix")
  outputOption.setRequired(true)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite output if exists")
  overwriteOption.setRequired(false)

  def getJobClassName(): String = {
    classOf[ConvertVitalCsvToSequence].getCanonicalName    
  }

  def getJobName(): String = {
    "convert-csv-to-seq"
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
    
    val procedure = new ConvertCsvToSequenceProcedure(inputPaths, outputPath, overwrite, globalContext)
    
    val tasks = procedure.generateTasks()
    
    val handler = new TasksHandler()
    
    handler.handleTasksList(this, tasks)

    println("DONE")
    
  }
  

  def main(args: Array[String]): Unit = {
    
    _mainImpl(args)
     
  }
  
}
