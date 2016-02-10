package ai.vital.aspen.data

import org.apache.commons.cli.Options
import org.apache.commons.cli.Option
import ai.vital.aspen.job.AbstractJob
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import ai.vital.aspen.util.SetOnceHashMap
import com.typesafe.config.ConfigList
import ai.vital.aspen.groovy.data.SegmentImportProcedure
import ai.vital.aspen.groovy.data.SegmentExportProcedure
import ai.vital.aspen.job.TasksHandler
import spark.jobserver.SparkJobValidation
import spark.jobserver.SparkJobValid
import ai.vital.aspen.groovy.data.SegmentEmptyProcedure

class SegmentEmptyJob {}

object SegmentEmptyJob extends AbstractJob {
  
  val segmentIDOption   = new Option("sid", "segmentID", true, "segmentID option")
  segmentIDOption.setRequired(true)
  
  def getJobClassName(): String = {
    classOf[SegmentEmptyJob].getCanonicalName
  }

  def getJobName(): String = {
    "segment-empty"
  }
  
  def getOptions(): Options = {
    addJobServerOptions(
      new Options().
        addOption(masterOption).
        addOption(segmentIDOption).
        addOption(profileOption).
        addOption(profileConfigOption)
//        addOption(serviceKeyOption).
    )
  }

    def runJob(sc: SparkContext, jobConfig: Config): Any = {
    
    
    val globalContext = new SetOnceHashMap()
    
    
    val segmentID = jobConfig.getString(segmentIDOption.getLongOpt)
    
    println("SegmentID: " + segmentID)
    
    val service = openVitalService()
    
    val segmentEmptyProcedure = new SegmentEmptyProcedure(segmentID, globalContext)
 
    val tasks  = segmentEmptyProcedure.generateTasks();
    
    val handler = new TasksHandler()
    
    handler.handleTasksList(this, tasks)

    service.close()
    
    println("DONE")
    
  }
    

  override def subvalidate(sc : SparkContext, config : Config) : SparkJobValidation = {
 
    SparkJobValid
    
    
  }
  
  def main(args: Array[String]): Unit = {
    
    _mainImpl(args)
     
  }
}