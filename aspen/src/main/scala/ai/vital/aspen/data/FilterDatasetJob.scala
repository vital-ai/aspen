package ai.vital.aspen.data

import ai.vital.aspen.job.AbstractJob
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import ai.vital.aspen.groovy.data.FilterDatasetProcedure
import ai.vital.aspen.util.SetOnceHashMap
import ai.vital.aspen.job.TasksHandler
import ai.vital.aspen.groovy.data.tasks.FilterDatasetTask

class FilterDatasetJob {
}

object FilterDatasetJob extends AbstractJob {
  
  val inputOption  = new Option("i", "input", true, "input RDD[(String, Array[Byte])], either named RDD name (name:<name>) or <path> (no prefix), where path is a .vital.seq or .vital[.gz] file")
  inputOption.setRequired(true)
  
  val outputOption  = new Option("o", "output", true, "output RDD[(String, Array[Byte])], either named RDD name (name:<name>) or <path> (no prefix), where path is a .vital.seq or .vital[.gz] file")
  outputOption.setRequired(true)
  
  val queryOption = new Option("q", "query-file", true, "query builder file path")
  queryOption.setRequired(true)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite output file if exists")
  overwriteOption.setRequired(false)
  
	def getJobClassName(): String = {
	  classOf[FilterDatasetJob].getCanonicalName
	}

  def getJobName(): String = {
	  "filter-dataset"
	}

  def getOptions(): Options = {
	  addJobServerOptions(
        new Options()
        .addOption(inputOption)
        .addOption(queryOption)
        .addOption(outputOption)
        .addOption(masterOption)
        .addOption(overwriteOption)
    )
	}

  def runJob(sc: SparkContext, jobConfig: Config): Any = {

    val inputPath = jobConfig.getString(inputOption.getLongOpt)
    val builderPath = jobConfig.getString(queryOption.getLongOpt)
    val outputPath = jobConfig.getString(outputOption.getLongOpt)
    val overwrite = getBooleanOption(jobConfig, overwriteOption)
    
    println("Input path: " + inputPath)
    println("Output path: " + outputPath)
    println("Query builder path: " + builderPath)
    println("Overwrite: " + overwrite)
    
    val globalContext = new SetOnceHashMap
    
    val procedure = new FilterDatasetProcedure(inputPath, builderPath, outputPath, overwrite, globalContext)
    
    val handler = new TasksHandler()
    
    val tasks = procedure.generateTasks()
    
    handler.handleTasksList(this, tasks)
    
    var stats = globalContext.get(FilterDatasetTask.FILTER_DATASET_STATS)
    
    if(stats == null) stats = "(no filter-dataset stats)"
    
    println(stats)
    
    println("DONE")
    
    return stats
    
  }
 
    
  def main(args: Array[String]): Unit = {
    
     _mainImpl(args)
     
  }
}