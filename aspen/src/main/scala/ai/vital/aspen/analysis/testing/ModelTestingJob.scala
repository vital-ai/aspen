package ai.vital.aspen.analysis.testing

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

class ModelTestingJob {}

object ModelTestingJob extends AbstractJob {
  
   def main(args: Array[String]): Unit = {
    
    _mainImpl(args)
     
  }
  
  def getJobClassName(): String = {
    classOf[ModelTestingJob].getCanonicalName
  }
  
  def getJobName(): String = {
     "Model Testing Job"
  }

  
  //expects 20news messages
  val inputOption  = new Option("i", "input", true, "input RDD[(String, Array[Byte])], either named RDD name (name:<name>) or <path> (no prefix), where path is a .vital.seq")
  inputOption.setRequired(true)
  
  val modelOption = new Option("mod", "model", true, "model path (directory or a zip/jar file)")
  modelOption.setRequired(true)
  
//  val outputOption = new Option("o", "output", true, "optional output RDD[(String, Array[Byte])], either named RDD name (name:<name> or <path> (no prefix), where path is a .vital.seq")
//  outputOption.setRequired(true)
  
  def getOptions(): Options = {
    addJobServerOptions(
    new Options().addOption(masterOption)
      .addOption(inputOption)
      .addOption(modelOption)
      .addOption(profileOption)
      .addOption(profileConfigOption)
      .addOption(serviceKeyOption)
    )
  }
  
  def runJob(sc: SparkContext, jobConfig: Config): Any = {

    val globalContext = new SetOnceHashMap()
    
    val modelPath = jobConfig.getString(modelOption.getLongOpt)

    val inputName = jobConfig.getString(inputOption.getLongOpt)
    
    println("Input name/path: " + inputName)
    println("Model path: " + modelPath)
    println("service config: " + serviceConfig)
    println("service profile: " + serviceProfile_)

    
//    val mt2c = AspenGroovyConfig.get.modelType2Class
//    mt2c.putAll(getModelManagerMap())
//    
//    val modelManager = new ModelManager()
//    println("Loading model ...")
//    val aspenModel = modelManager.loadModel(modelPath.toUri().toString())
//    
//    println("Model loaded successfully")

    
    val procedure = new ModelTestingProcedure(inputName, modelPath, globalContext)
    
    val tasks = procedure.generateTasks()
    
    val handler = new TasksHandler()
    
    handler.handleTasksList(this, tasks)
    
    
    var output : Object = globalContext.get(TestModelTask.STATS_STRING)
    
    if(output == null) output = "(no stats)";
    
    println(output.toString())
    
    unloadDynamicDomains()
    
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
