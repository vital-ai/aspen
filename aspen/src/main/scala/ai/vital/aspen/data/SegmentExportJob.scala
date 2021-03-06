package ai.vital.aspen.data

import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.spark.SparkContext

import com.typesafe.config.Config

import ai.vital.aspen.groovy.data.SegmentExportProcedure
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.job.TasksHandler
import ai.vital.aspen.util.SetOnceHashMap
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobValidation

class SegmentExportJob {}

object SegmentExportJob extends AbstractJob {
  
  val segmentIDOption   = new Option("sid", "segmentID", true, "segmentID option")
  segmentIDOption.setRequired(true)
  
  val outputOption  = new Option("o", "output", true, "output vital sql .vital.csv[.gz] or .vital.seq")
  outputOption.setRequired(true)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite output if exists")
  overwriteOption.setRequired(false)
  
  def getJobClassName(): String = {
    classOf[SegmentExportJob].getCanonicalName
  }

  def getJobName(): String = {
    "segment-export"
  }
  
  def getOptions(): Options = {
    addJobServerOptions(
      new Options().
        addOption(masterOption).
        addOption(outputOption).
        addOption(segmentIDOption).
        addOption(profileOption).
        addOption(profileConfigOption).
//        addOption(serviceKeyOption).
        addOption(overwriteOption)
    )
  }

    def runJob(sc: SparkContext, jobConfig: Config): Any = {
    
    
    val globalContext = new SetOnceHashMap()
    
    
    val outputPath = jobConfig.getString(outputOption.getLongOpt)
    val segmentID = jobConfig.getString(segmentIDOption.getLongOpt)
    
    println("Output path: " + outputPath)
    println("SegmentID: " + segmentID)
    
    val overwrite = getBooleanOption(jobConfig, overwriteOption)
    
    println("Overwrite ? " + overwrite)
    
    val segmentExportProcedure = new SegmentExportProcedure(segmentID, outputPath, overwrite, globalContext)
 
    val tasks  = segmentExportProcedure.generateTasks();
    
    val handler = new TasksHandler()
    
    handler.handleTasksList(this, tasks)

    println("DONE")
    
  }
    

  override def subvalidate(sc : SparkContext, config : Config) : SparkJobValidation = {
 
    SparkJobValid
    
    
  }
  
  def main(args: Array[String]): Unit = {
    
    _mainImpl(args)
     
  }
}