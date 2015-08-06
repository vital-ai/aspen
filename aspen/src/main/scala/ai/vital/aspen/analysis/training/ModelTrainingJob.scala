package ai.vital.aspen.analysis.training

import java.util.Arrays
import java.util.Date

import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import com.typesafe.config.Config

import ai.vital.aspen.groovy.modelmanager.AspenModel
import ai.vital.aspen.groovy.modelmanager.ModelTaxonomySetter
import ai.vital.aspen.groovy.predict.ModelTrainingProcedure
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.job.TasksHandler
import ai.vital.aspen.model.AspenLinearRegressionModel
import ai.vital.aspen.model.PredictionModel
import ai.vital.aspen.util.SetOnceHashMap
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.vitalsigns.VitalSigns
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import spark.jobserver.SparkJobInvalid
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobValidation

class ModelTrainingJob {}

object ModelTrainingJob extends AbstractJob {
  
  val modelBuilderOption = new Option("b", "model-builder", true, "model builder file")
  
  modelBuilderOption.setRequired(true)
  
  //expects 20news messages
  val inputOption  = new Option("i", "input", true, "input RDD[(String, Array[Byte])], either named RDD name (name:<name>) or <path> (no prefix), where path is a .vital.seq or .vital file")
  inputOption.setRequired(false)
  
  val outputOption = new Option("mod", "model", true, "output model path (directory)")
  outputOption.setRequired(true)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite model if exists")
  overwriteOption.setRequired(false)
  
  
  def getOptions(): Options = {
    addJobServerOptions(
      new Options()
      .addOption(masterOption)
      .addOption(modelBuilderOption)
      .addOption(inputOption)
      .addOption(outputOption)
      .addOption(overwriteOption)
      .addOption(profileOption)
    )
  }
  
  
  def main(args: Array[String]): Unit = {
    
     _mainImpl(args)
     
  }
  
  def getJobClassName(): String = {
    classOf[ModelTrainingJob].getCanonicalName
  }

  def getJobName(): String = {
    "Model Training Job" 
  }

  
  def runJob(sc: SparkContext, jobConfig: Config): Any = {
		  
    var inputName = getOptionalString(jobConfig, inputOption)
    
    val modelPathParam = jobConfig.getString(outputOption.getLongOpt)
    
    val outputModelPath = new Path(modelPathParam)
    val overwrite = getBooleanOption(jobConfig, overwriteOption)
    val builderPath = new Path(jobConfig.getString(modelBuilderOption.getLongOpt))
    
    
    println("input train name: " + inputName)
    println("builder path: " + builderPath)
    println("output model path: " + outputModelPath)
    println("overwrite if exists: " + overwrite)
    println("service profile: " + serviceProfile)
    
    if(isNamedRDDSupported()) {
    	println("named RDDs supported")
    } else {
    	println("named RDDs not supported, storing RDD references in a local cache")
    }
    
    val modelCreator = getModelCreator()
    
    val builderFS = FileSystem.get(builderPath.toUri(), hadoopConfiguration)
    if(!builderFS.exists(builderPath)) {
      throw new RuntimeException("Builder file not found: " + builderPath.toString())
    }
    
    val builderStatus = builderFS.getFileStatus(builderPath)
    if(!builderStatus.isFile()) {
      throw new RuntimeException("Builder path does not denote a file: " + builderPath.toString())
    }
    
    
    val buildInputStream = builderFS.open(builderPath)
    val builderBytes = IOUtils.toByteArray(buildInputStream)
    buildInputStream.close()
    
    //not loaded!
    val aspenModel = modelCreator.createModel(builderBytes)
    
    if(inputName == null) {
      
      if( aspenModel.getModelConfig.getSourceDatasets == null || aspenModel.getModelConfig.getSourceDatasets.size() == 0 ) {
        throw new RuntimeException("no input parameter nor source dataset param in builder file")
      }
      
      if(aspenModel.getModelConfig.getSourceDatasets.size() > 1) {
        println("Ignoring sourceDatasets other than first ")
      }
      
      inputName = aspenModel.getModelConfig.getSourceDatasets.get(0)
      
      println("inputName from builder: " + inputName)
      
    } else {
      
      aspenModel.getModelConfig.setSourceDatasets(Arrays.asList(inputName))
      
    }
    
    val creatorMap = getCreatorMap()
    
    if( !creatorMap.containsKey( aspenModel.getType ) ) {
      throw new RuntimeException("only the following model types are supported: " + creatorMap.keySet().toString())
    }
    
    
    //create training class instance, it will validate the algorithm params

    
    if(serviceProfile != null) {
        VitalServiceFactory.setServiceProfile(serviceProfile)
    }
    
    VitalSigns.get.setVitalService(VitalServiceFactory.getVitalService)
    
    ModelTaxonomySetter.loadTaxonomies(aspenModel, null)

      //use full set
//      trainRDD = inputRDD
      
//    } else {
//      
//      val splits = inputBlockRDD.randomSplit(Array(0.6, 0.4), seed = 11L)
//          
//      trainRDD = splits(0)
//          
//      testRDD = splits(1)
//      
//    }
    
    
    var globalContext = new SetOnceHashMap()
    
    loadDynamicDomainJars(aspenModel)
    
    val procedure = new ModelTrainingProcedure(aspenModel, inputName, modelPathParam, overwrite, globalContext)
    
//    procedure.inputPath = in
    
    val tasks = procedure.generateTasks()
    
    val handler = new TasksHandler()
    
    handler.handleTasksList(this, tasks)
    
    var response : String = null

    if(aspenModel.isInstanceOf[PredictionModel]) {
      
      response = aspenModel.asInstanceOf[PredictionModel].getError()
      
    }
    
    if(response == null) response  = "DONE " + new Date().toString()
    
    return response
		  
  }
  
  def vectorizeNoLabels ( trainRDD: RDD[(String, Array[Byte])], model: AspenModel ) : RDD[Vector] = {
    
    val vectorized = trainRDD.map { pair =>
      
      val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
            
      val vitalBlock = new VitalBlock(inputObjects)
      
      val ex = model.getFeatureExtraction
      
      val featuresMap = ex.extractFeatures(vitalBlock)

      model.asInstanceOf[ai.vital.aspen.model.PredictionModel].vectorizeNoLabels(vitalBlock, featuresMap);
      
    }
    
    return vectorized
    
  }
  
  
  def vectorize (trainRDD: RDD[(String, Array[Byte])], model: AspenModel) : RDD[LabeledPoint] = {

    val vectorized = trainRDD.map { pair =>
      
      val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
            
      val vitalBlock = new VitalBlock(inputObjects)
      
      val ex = model.getFeatureExtraction
      
      val featuresMap = ex.extractFeatures(vitalBlock)

      model.asInstanceOf[ai.vital.aspen.model.PredictionModel].vectorizeLabels(vitalBlock, featuresMap);
      
    }
    
    return vectorized
    
  }

  //this is a special vectorization
  def vectorizeWithScaling (trainRDD: RDD[(String, Array[Byte])], model: AspenLinearRegressionModel) : RDD[LabeledPoint] = {
		
		  val vectorized = trainRDD.map { pair =>
		  
  		  val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
  		  
  		  val vitalBlock = new VitalBlock(inputObjects)
  		  
  		  val ex = model.getFeatureExtraction
  		  
  		  val featuresMap = ex.extractFeatures(vitalBlock)
  		  
  		  model.vectorizeLabelsNoScaling(vitalBlock, featuresMap);
		  
		  }
      
      //scaler initialized, we may now vectorize as usual
      model.initScaler(vectorized)

      //TODO use the input vectorized values for performance gains?
      val vectorized2 = trainRDD.map { pair =>
      
        val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
        
        val vitalBlock = new VitalBlock(inputObjects)
        
        val ex = model.getFeatureExtraction
        
        val featuresMap = ex.extractFeatures(vitalBlock)
        
        val notScaled = model.vectorizeLabels(vitalBlock, featuresMap);
        
        model.scaleLabeledPoint(notScaled)
      
      }
		  return vectorized2
				  
  }
  
  override def subvalidate(sc: SparkContext, config: Config) : SparkJobValidation = {
    
    val inputValue = getOptionalString(config, inputOption)
    
    if(inputValue != null) {
    
      if(!skipNamedRDDValidation && inputValue.startsWith("name:")) {
        
        val inputRDDName = inputValue.substring("name:".length)
        
        if(!isNamedRDDSupported()) {
      	  return new SparkJobInvalid("Cannot use named RDD output - no spark job context")
        }
        
        val inputRDD = this.namedRdds.get[(String, Array[Byte])](inputRDDName)
  //      val rdd = this.namedRdds.get[(Long, scala.Seq[String])]("dictionary")
        
        if( !inputRDD.isDefined ) SparkJobInvalid("Missing named RDD [" + inputRDDName + "]")
          
        
      }
    
    }
    
    SparkJobValid
    
  }
 
}