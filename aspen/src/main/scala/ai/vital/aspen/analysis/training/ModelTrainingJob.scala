package ai.vital.aspen.analysis.training

import java.util.Arrays
import java.util.Date
import java.util.HashMap
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import com.typesafe.config.Config

import ai.vital.aspen.analysis.training.impl.CalculateAggregationValueTaskImpl
import ai.vital.aspen.analysis.training.impl.CollectCategoricalFeatureTaxonomyDataTaskImpl
import ai.vital.aspen.analysis.training.impl.CollectNumericalFeatureDataTaskImpl
import ai.vital.aspen.analysis.training.impl.CollectTextFeatureDataTaskImpl
import ai.vital.aspen.analysis.training.impl.CollectTrainTaxonomyDataTaskImpl
import ai.vital.aspen.analysis.training.impl.CountDatasetTaskImpl
import ai.vital.aspen.analysis.training.impl.LoadDataSetTaskImpl
import ai.vital.aspen.analysis.training.impl.SplitDatasetTaskImpl
import ai.vital.aspen.analysis.training.impl.TestModelTaskImpl
import ai.vital.aspen.analysis.training.impl.TrainModelTaskImpl
import ai.vital.aspen.groovy.modelmanager.AspenModel
import ai.vital.aspen.groovy.modelmanager.ModelTaxonomySetter
import ai.vital.aspen.groovy.predict.ModelTrainingProcedure
import ai.vital.aspen.groovy.predict.tasks.CalculateAggregationValueTask
import ai.vital.aspen.groovy.predict.tasks.CollectCategoricalFeatureTaxonomyDataTask
import ai.vital.aspen.groovy.predict.tasks.CollectNumericalFeatureDataTask
import ai.vital.aspen.groovy.predict.tasks.CollectTextFeatureDataTask
import ai.vital.aspen.groovy.predict.tasks.CollectTrainTaxonomyDataTask
import ai.vital.aspen.groovy.predict.tasks.CountDatasetTask
import ai.vital.aspen.groovy.predict.tasks.LoadDataSetTask
import ai.vital.aspen.groovy.predict.tasks.SaveModelTask
import ai.vital.aspen.groovy.predict.tasks.SplitDatasetTask
import ai.vital.aspen.groovy.predict.tasks.TestModelTask
import ai.vital.aspen.groovy.predict.tasks.TrainModelTask
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.model.PredictionModel
import ai.vital.aspen.model.AspenLinearRegressionModel
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
  
  //this is only used when namedRDDs support is disabled
  var datasetsMap : java.util.HashMap[String, RDD[(String, Array[Byte])]] = null;
  
  var hadoopConfig : Configuration = null
  
  var globalContext : SetOnceHashMap = null
  
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
    
    var modelPathParam = jobConfig.getString(outputOption.getLongOpt)
    
    var zipContainer = false
    var jarContainer = false
    
    var outputContainerPath : Path = null
    
    if(modelPathParam.endsWith(".jar")) {
      outputContainerPath = new Path(modelPathParam)
      modelPathParam = modelPathParam.substring(0, modelPathParam.length() - 4)
      jarContainer = true
    } else if(modelPathParam.endsWith(".zip")) {
      outputContainerPath = new Path(modelPathParam)
      modelPathParam = modelPathParam.substring(0, modelPathParam.length() - 4)
      zipContainer = true
    }
    
    val outputModelPath = new Path(modelPathParam)
    val overwrite = getBooleanOption(jobConfig, overwriteOption)
    val builderPath = new Path(jobConfig.getString(modelBuilderOption.getLongOpt))
    val serviceProfile = getOptionalString(jobConfig, profileOption)
    
    
    println("input train name: " + inputName)
    println("builder path: " + builderPath)
    println("output model path: " + outputModelPath)
    if(zipContainer) println("   output is a zip container (.zip)")
    if(jarContainer) println("   output is a jar container (.jar)")
    println("overwrite if exists: " + overwrite)
    println("service profile: " + serviceProfile)
    
    if(isNamedRDDSupported()) {
    	println("named RDDs supported")
    } else {
    	println("named RDDs not supported, storing RDD references in a local cache")
    	datasetsMap = new java.util.HashMap[String, RDD[(String, Array[Byte])]]();
    }
    
    val modelCreator = getModelCreator()
    
    hadoopConfig = new Configuration()
    
    globalContext = new SetOnceHashMap()
    
    val builderFS = FileSystem.get(builderPath.toUri(), hadoopConfig)
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
    
    ModelTaxonomySetter.loadTaxonomies(aspenModel.getModelConfig, null)

    val modelFS = FileSystem.get(outputModelPath.toUri(), hadoopConfig)

    
    if (modelFS.exists(outputModelPath) || (outputContainerPath != null && modelFS.exists(outputContainerPath) ) ) {
      
      if( !overwrite ) {
    	  throw new RuntimeException("Output model path already exists, use -ow option")
      }
      
      modelFS.delete(outputModelPath, true)
      if(outputContainerPath != null) {
    	  modelFS.delete(outputContainerPath, true)
      }
      
    }
    
    var inputRDDName : String = null
    if(inputName.startsWith("name:")) {
      inputRDDName = inputName.substring("name:".length())
    }
    
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
    
    
    val commandParams = new HashMap[String, String]()
    commandParams.put(inputOption.getLongOpt, inputName)
    
    val procedure = new ModelTrainingProcedure(aspenModel, commandParams, globalContext)
    
//    procedure.inputPath = in
    
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
      
      var taskImpl : AbstractModelTrainingTaskImpl[_] = null
      
      if(task.isInstanceOf[CalculateAggregationValueTask]) {
        
        taskImpl = new CalculateAggregationValueTaskImpl(sc, task.asInstanceOf[CalculateAggregationValueTask])
        
      } else if(task.isInstanceOf[CollectCategoricalFeatureTaxonomyDataTask]) {
        
        taskImpl = new CollectCategoricalFeatureTaxonomyDataTaskImpl(sc, task.asInstanceOf[CollectCategoricalFeatureTaxonomyDataTask])
        
      } else if(task.isInstanceOf[CollectNumericalFeatureDataTask]) {

        taskImpl = new CollectNumericalFeatureDataTaskImpl(sc, task.asInstanceOf[CollectNumericalFeatureDataTask])
        
      } else if(task.isInstanceOf[CollectTextFeatureDataTask]) {
        
        taskImpl = new CollectTextFeatureDataTaskImpl(sc, task.asInstanceOf[CollectTextFeatureDataTask])
        
      } else if(task.isInstanceOf[CollectTrainTaxonomyDataTask]) {
        
        taskImpl = new CollectTrainTaxonomyDataTaskImpl(sc, task.asInstanceOf[CollectTrainTaxonomyDataTask])
        
      } else if(task.isInstanceOf[CountDatasetTask]) {
        
        taskImpl = new CountDatasetTaskImpl(sc, task.asInstanceOf[CountDatasetTask])
        
      } else if(task.isInstanceOf[LoadDataSetTask]) {
        
        taskImpl = new LoadDataSetTaskImpl(sc, task.asInstanceOf[LoadDataSetTask], serviceProfile)
        
      } else if(task.isInstanceOf[SaveModelTask]) {
        
        val smt = task.asInstanceOf[SaveModelTask]
        
        //model packaging is now implemented in the model itsefl
    
        if(outputContainerPath != null) {
          aspenModel.persist(modelFS, outputContainerPath, zipContainer || jarContainer)
        } else {
          aspenModel.persist(modelFS, outputModelPath, zipContainer || jarContainer)
        }
        
        //no output
        
      } else if(task.isInstanceOf[SplitDatasetTask]) {
        
        taskImpl = new SplitDatasetTaskImpl(sc, task.asInstanceOf[SplitDatasetTask])
        
      } else if(task.isInstanceOf[TrainModelTask]) {

        taskImpl = new TrainModelTaskImpl(sc, task.asInstanceOf[TrainModelTask])

      } else if(task.isInstanceOf[TestModelTask]) {

        taskImpl = new TestModelTaskImpl(sc, task.asInstanceOf[TestModelTask])
        
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
    
    
    var response : String = null

    if(aspenModel.isInstanceOf[PredictionModel]) {
      
      response = aspenModel.asInstanceOf[PredictionModel].getError()
      
    }
    
    if(response == null) response  = "DONE " + new Date().toString()
    
    return response
		  
  }
  
  def addToZipFile(zos: ZipOutputStream, modelFS: FileSystem, filePath: Path ) : Unit = {
    
    val entry = new ZipEntry(filePath.getName)
    zos.putNextEntry(entry)
    val stream = modelFS.open(filePath)
    IOUtils.copy(stream, zos)
    stream.close()
    zos.closeEntry()
    
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
 
  def getDataset(datasetName :String) : RDD[(String, Array[Byte])] = {
    
    if(isNamedRDDSupported()) {
      
    	val ds = this.namedRdds.get[(String, Array[Byte])](datasetName)
    			
 			if(!ds.isDefined) throw new RuntimeException("Dataset not loaded: " + datasetName)
    	
    	return ds.get;
      
    } else {
      
    	val ds = datasetsMap.get(datasetName)
    			
    	if(ds == null) throw new RuntimeException("Dataset not loaded: " + datasetName)
    	
    	return ds;
      
    }
    
  }
  
}