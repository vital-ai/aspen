package ai.vital.aspen.analysis.predict

import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import ai.vital.aspen.groovy.predict.ModelTestingProcedure
import ai.vital.aspen.groovy.predict.tasks.TestModelTask
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.job.TasksHandler
import ai.vital.aspen.util.SetOnceHashMap
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.vitalsigns.VitalSigns
import spark.jobserver.SparkJobInvalid
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobValidation
import ai.vital.aspen.groovy.predict.ModelPredictProcedure
import ai.vital.aspen.groovy.predict.tasks.ModelPredictTask

class ModelPredictJob {}

object ModelPredictJob extends AbstractJob {
  
   def main(args: Array[String]): Unit = {
    
    _mainImpl(args)
     
  }
  
  def getJobClassName(): String = {
    classOf[ModelPredictJob].getCanonicalName
  }
  
  def getJobName(): String = {
     "Model Predict Job"
  }

  
  //expects 20news messages
  val inputOption  = new Option("i", "input", true, "input RDD[(String, Array[Byte])], either named RDD name (name:<name>) or <path> (no prefix), where path is a .vital.seq or .vital.gz")
  inputOption.setRequired(true)
  
  val outputOption  = new Option("o", "output", true, "output RDD[(String, Array[Byte])], either named RDD name (name:<name>) or <path> (no prefix), where path is a .vital.seq or .vital.gz")
  inputOption.setRequired(true)
  
  val modelOption = new Option("mod", "model", true, "model path (directory or a zip/jar file)")
  modelOption.setRequired(true)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite model if exists")
  overwriteOption.setRequired(false)
  
//  val outputOption = new Option("o", "output", true, "optional output RDD[(String, Array[Byte])], either named RDD name (name:<name> or <path> (no prefix), where path is a .vital.seq")
//  outputOption.setRequired(true)
  
  def getOptions(): Options = {
    addJobServerOptions(
    new Options().addOption(masterOption)
      .addOption(inputOption)
      .addOption(overwriteOption)
      .addOption(outputOption)
      .addOption(modelOption)
      .addOption(profileOption)
    )
  }
  
  def runJob(sc: SparkContext, jobConfig: Config): Any = {

    val globalContext = new SetOnceHashMap()
    
    val modelPath = jobConfig.getString(modelOption.getLongOpt)

    val inputPath = jobConfig.getString(inputOption.getLongOpt)
    
    val outputPath = jobConfig.getString(outputOption.getLongOpt)
    
    val overwrite = getBooleanOption(jobConfig, overwriteOption)
    
    println("Model path: " + modelPath)
    println("Input  name/path: " + inputPath)
    println("Output name/path: " + outputPath)
    println("Overwrite output ? " + overwrite)
    println("service profile: " + serviceProfile)

    
    if(serviceProfile != null) {
        VitalServiceFactory.setServiceProfile(serviceProfile)
    }
    
    VitalSigns.get.setVitalService(VitalServiceFactory.getVitalService)
    
    val procedure = new ModelPredictProcedure(inputPath, modelPath, outputPath, overwrite, globalContext)
    
    val tasks = procedure.generateTasks()
    
    val handler = new TasksHandler()
    
    handler.handleTasksList(this, tasks)
    
    
    var output : Object = globalContext.get(ModelPredictTask.STATS_STRING)
    
    if(output == null) output = "(no stats)";
    
    println(output.toString())
    
    return output
  
  }
  
  override def subvalidate(sc: SparkContext, config: Config) : SparkJobValidation = {
    
    val inputValue = config.getString(inputOption.getLongOpt)
    
    if(!skipNamedRDDValidation && inputValue.startsWith("name:")) {
      
      val inputRDDName = inputValue.substring("name:".length)
      
      try{
        if(this.namedRdds == null) {
        } 
      } catch { case ex: NullPointerException => {
        return new SparkJobInvalid("Cannot use named RDD output - no spark job context")
        
      }}
      
      val inputRDD = this.namedRdds.get[(String, Array[Byte])](inputRDDName)
//      val rdd = this.namedRdds.get[(Long, scala.Seq[String])]("dictionary")
      
      if( !inputRDD.isDefined ) SparkJobInvalid("Missing named RDD [" + inputRDDName + "]")
        
      
    }
    
    SparkJobValid
    
  }
}
