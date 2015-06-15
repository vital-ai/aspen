package ai.vital.aspen.analysis.training

import ai.vital.aspen.job.AbstractJob
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import ai.vital.hadoop.writable.VitalBytesWritable
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text
import ai.vital.vitalsigns.VitalSigns
import scala.collection.JavaConversions._
import org.example.twentynews.domain.Message
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.clustering.KMeans
import java.util.Date
import java.util.Set
import java.util.HashSet
import ai.vital.domain.EntityInstance
import ai.vital.vitalsigns.model.property.MultiValueProperty
import ai.vital.vitalsigns.model.property.IProperty
import ai.vital.predictmodel.Aggregate
import ai.vital.predictmodel.Aggregate.Function
import java.util.Collection
import java.util.HashMap
import ai.vital.aspen.groovy.modelmanager.ModelCreator
import ai.vital.aspen.groovy.modelmanager.AspenModel
import ai.vital.aspen.model.RandomForestPredictionModel
import ai.vital.aspen.model.RandomForestRegressionModel
import ai.vital.aspen.model.DecisionTreePredictionModel
import ai.vital.aspen.model.KMeansPredictionModel
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import ai.vital.aspen.model.Features
import ai.vital.aspen.model.Features
import ai.vital.domain.TargetNode
import groovy.lang.GString
import java.util.zip.ZipOutputStream
import java.util.zip.ZipEntry
import ai.vital.aspen.groovy.modelmanager.ModelManager
import org.apache.commons.io.IOUtils
import java.io.ByteArrayInputStream
import ai.vital.aspen.model.NaiveBayesPredictionModel
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.classification.NaiveBayes
import spark.jobserver.SparkJobInvalid
import spark.jobserver.SparkJobValidation
import spark.jobserver.SparkJobValid
import ai.vital.vitalservice.query.VitalQuery
import ai.vital.query.querybuilder.VitalBuilder
import ai.vital.vitalservice.segment.VitalSegment
import java.util.ArrayList
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalsigns.model.GraphMatch
import ai.vital.vitalservice.query.VitalGraphQuery
import ai.vital.vitalsigns.model.property.URIProperty
import ai.vital.vitalsigns.block.CompactStringSerializer
import ai.vital.predictmodel.CategoricalFeature
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.aspen.groovy.featureextraction.PredictionModelAnalyzer
import ai.vital.aspen.groovy.featureextraction.FeatureExtraction
import org.apache.spark.Accumulator
import ai.vital.predictmodel.TextFeature
import ai.vital.aspen.groovy.featureextraction.CategoricalFeatureData
import ai.vital.aspen.groovy.featureextraction.Dictionary
import ai.vital.aspen.groovy.featureextraction.TextFeatureData
import ai.vital.aspen.groovy.featureextraction.FeatureData
import ai.vital.predictmodel.NumericalFeature
import ai.vital.aspen.groovy.featureextraction.NumericalFeatureData
import ai.vital.predictmodel.Taxonomy
import ai.vital.aspen.groovy.predict.ModelTrainingProcedure
import ai.vital.aspen.groovy.predict.ModelTrainingTask
import ai.vital.aspen.groovy.predict.tasks.CalculateAggregationValueTask
import ai.vital.aspen.groovy.predict.tasks.CollectNumericalFeatureDataTask
import ai.vital.aspen.groovy.predict.tasks.CollectTextFeatureDataTask
import ai.vital.aspen.groovy.predict.tasks.CollectCategoricalFeatureTaxonomyDataTask
import ai.vital.aspen.groovy.predict.tasks.CollectTrainTaxonomyDataTask
import ai.vital.aspen.groovy.predict.tasks.CountDatasetTask
import ai.vital.aspen.groovy.predict.tasks.LoadDataSetTask
import ai.vital.aspen.groovy.predict.tasks.SaveModelTask
import ai.vital.aspen.groovy.predict.tasks.SplitDatasetTask
import ai.vital.aspen.groovy.predict.tasks.TrainModelTask
import ai.vital.aspen.groovy.predict.tasks.TestModelTask
import ai.vital.predictmodel.Feature
import ai.vital.aspen.model.PredictionModel
import ai.vital.aspen.util.SetOnceHashMap
import ai.vital.aspen.model.CollaborativeFilteringPredictionModel
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import java.util.Arrays
import org.apache.spark.mllib.classification.SVMWithSGD
import ai.vital.aspen.model.SparkLinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import ai.vital.vitalsigns.model.URIReference
import org.apache.spark.mllib.feature.StandardScaler
import ai.vital.vitalsigns.model.VITAL_Category
import ai.vital.vitalsigns.model.VITAL_Container
import ai.vital.vitalsigns.model.Edge_hasChildCategory
import ai.vital.aspen.groovy.modelmanager.ModelTaxonomySetter

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
      
      
      if(task.isInstanceOf[CalculateAggregationValueTask]) {
        
        val cavt = task.asInstanceOf[CalculateAggregationValueTask]

        checkDependencies_calculateAggregationValue(sc, cavt)
        
        calculateAggregationValue(sc, cavt)
        
//      } else if(task.isInstanceOf[Collect])
      } else if(task.isInstanceOf[CollectCategoricalFeatureTaxonomyDataTask]) {
        
        val ccftdt = task.asInstanceOf[CollectCategoricalFeatureTaxonomyDataTask]
        
        checkDependencies_collectCategoricalFeatureTaxonomyDataTask(sc, ccftdt)
        
        collectCategoricalFeatureTaxonomyDataTask(sc, ccftdt)
        
      } else if(task.isInstanceOf[CollectNumericalFeatureDataTask]) {
        
        val cnfdt = task.asInstanceOf[CollectNumericalFeatureDataTask]
        
        checkDependencies_collectNumericalFeatureDataTask(sc, cnfdt)
        
        collectNumericalFeatureDataTask(sc, cnfdt)
        
      } else if(task.isInstanceOf[CollectTextFeatureDataTask]) {
        
        val ctfdt = task.asInstanceOf[CollectTextFeatureDataTask]
        
        checkDependencies_collectTextFeatureData(sc, ctfdt)
        
        collectTextFeatureData(sc, ctfdt)
        
      } else if(task.isInstanceOf[CollectTrainTaxonomyDataTask]) {
        
        val ctdt = task.asInstanceOf[CollectTrainTaxonomyDataTask]
        
        checkDependencies_collectTrainTaxonomyDataTask(sc, ctdt)
        
        collectTrainTaxonomyDataTask(sc, ctdt)
        
      } else if(task.isInstanceOf[CountDatasetTask]) {
        
        val cdt = task.asInstanceOf[CountDatasetTask]
        
        checkDependencies_countDataset(sc, cdt);
        
        countDataset(sc, cdt);
        
//        val docsCount = countDataset(sc, aspenModel, ctst.datasetName)
//        
//        println("Documents count: " + docsCount)
//        
//        ctst.result = docsCount.intValue()

      } else if(task.isInstanceOf[LoadDataSetTask]) {
        
        val ldt = task.asInstanceOf[LoadDataSetTask]
        
        checkDependencies_loadDataset(sc, ldt)
        
        loadDataset(sc, ldt, serviceProfile)
        
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
        
        val sdt = task.asInstanceOf[SplitDatasetTask]
        
        checkDependencies_splitDataset(sc, sdt)
        
        splitDataset(sc, sdt)
        
      } else if(task.isInstanceOf[TrainModelTask]) {

        val tmt = task.asInstanceOf[TrainModelTask]
        
        checkDependencies_trainModel(sc, tmt)
        
        println("Training model...")
        
        trainModel(sc, tmt)

      } else if(task.isInstanceOf[TestModelTask]) {

        var tmt = task.asInstanceOf[TestModelTask]
        
        checkDependencies_testModel(sc, tmt)
        
        testModel(sc, tmt)
        
        
      } else {
        throw new RuntimeException("Unhandled task: " + task.getClass.getCanonicalName);
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
  def vectorizeWithScaling (trainRDD: RDD[(String, Array[Byte])], model: SparkLinearRegressionModel) : RDD[LabeledPoint] = {
		
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
  
  def checkDependencies_calculateAggregationValue(sc : SparkContext, cavt: CalculateAggregationValueTask) : Unit = {
  
    val datasetName = cavt.datasetName;
    val trainRDD = getDataset(datasetName)
    
  }
  
  def calculateAggregationValue(sc : SparkContext, cavt: CalculateAggregationValueTask) : Unit = {
 
    val datasetName = cavt.datasetName;
    
    val a = cavt.aggregate
    
    var acc : Accumulator[Int] = null; 
      
    if(a.getFunction == Function.AVERAGE) {
          acc = sc.accumulator(0);
    }
    
    val aspenModel = cavt.getModel
    
    val trainRDD = getDataset(datasetName)
        
        val numerics = trainRDD.map { pair =>
              
          val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
              
          val vitalBlock = new VitalBlock(inputObjects)
              
          val aggFunctions = PredictionModelAnalyzer.getAggregationFunctions(aspenModel.getModelConfig, a)
          
          val ex = aspenModel.getFeatureExtraction
              
          val features = ex.extractFeatures(vitalBlock, aggFunctions)
              
          val fv = features.get( a.getRequires().get(0) )
          
          if(fv == null) {
            0d
          } else {
            if( ! fv.isInstanceOf[Number] ) {
              throw new RuntimeException("Expected double value")
            }
            
            if(acc != null) {
              acc += 1
            }
            
            fv.asInstanceOf[Number].doubleValue()
          }
              
        }
      
        var aggV : java.lang.Double = null
        
        if(a.getFunction == Function.AVERAGE) {
          
          if(acc.value == 0) {
            
            aggV = 0d;
            
          } else {
            
            val reduced = numerics.reduce { (a1, a2) =>
              a1 + a2
            }
            
            aggV = reduced /acc.value
            
          } 
          
          
        } else if(a.getFunction == Function.SUM) {
          aggV = numerics.sum()
        } else if(a.getFunction == Function.MIN) {
          aggV = numerics.min()
        } else if(a.getFunction == Function.MAX) {
          aggV = numerics.max()
        } else {
          throw new RuntimeException("Unhandled aggregation function: " + a.getFunction)
        }

        aspenModel.getAggregationResults.put(a.getProvides, aggV)
    
        globalContext.put(a.getProvides + CalculateAggregationValueTask.AGGREGATE_VALUE_SUFFIX, aggV);
        
  }
  
  def checkDependencies_collectTrainTaxonomyDataTask(sc: SparkContext, ctct : CollectTrainTaxonomyDataTask): Unit = {
    val trainRDD = getDataset(ctct.datasetName)       
  }
  
  def collectTrainTaxonomyDataTask(sc: SparkContext, ctct : CollectTrainTaxonomyDataTask): Unit = {
 
    val aspenModel = ctct.getModel
    
    if(aspenModel.getType.equals(KMeansPredictionModel.spark_kmeans_prediction)) {
          
      return
          
    } else {
    
        val trainRDD = getDataset(ctct.datasetName)       
      
        //gather target categories
        val categoriesRDD = trainRDD.map { pair =>
            val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
            
            val vitalBlock = new VitalBlock(inputObjects)
            
            val ex = aspenModel.getFeatureExtraction
            
            val featuresMap = ex.extractFeatures(vitalBlock)
                
            val f = aspenModel.getModelConfig.getTrainFeature.getFunction
            f.rehydrate(ex, ex, ex)
            val category = f.call(vitalBlock, featuresMap)
                
            if(category == null) throw new RuntimeException("No category returned: " + pair._1)
            
            val c = category.asInstanceOf[VITAL_Category]
            
            (c.getURI, c)
                
        }
        
        val categories = categoriesRDD.reduceByKey( (c1: VITAL_Category , c2: VITAL_Category) => c1 ).map(p => p._2).collect()
        
        println("categories count: " + categories.size)
            
        val taxonomy = new Taxonomy()
        
        val rootCategory = new VITAL_Category()
        rootCategory.setURI("urn:taxonomy-root")
        rootCategory.setProperty("name", "Taxonomy Root")
        
        var container = new VITAL_Container()
        container.putGraphObject(rootCategory)
        
        taxonomy.setRootCategory(rootCategory)
        taxonomy.setRoot(rootCategory.getURI)
        taxonomy.setContainer(container)
        
        var c = 0
        for(x <- categories) {
          c = c+1
          container.putGraphObject(x)
          val edge = new Edge_hasChildCategory()
          edge.addSource(rootCategory).addDestination(x).setURI("urn:Edge_hasChildCategory_" + rootCategory.getURI + "_" + c)
          container.putGraphObject(edge)
        }
        
        val trainedCategories = CategoricalFeatureData.fromTaxonomy(taxonomy)
        aspenModel.setTrainedCategories(trainedCategories)
              
        globalContext.put(CollectTrainTaxonomyDataTask.TRAIN_TAXONOMY_DATA, trainedCategories)
    }
    
  }
  
  def checkDependencies_collectCategoricalFeatureTaxonomyDataTask(sc: SparkContext, ctct : CollectCategoricalFeatureTaxonomyDataTask): Unit = {
      val trainRDD = getDataset(ctct.datasetName)       
  }
  
  
  def collectCategoricalFeatureTaxonomyDataTask(sc: SparkContext, ctct : CollectCategoricalFeatureTaxonomyDataTask): Unit = {
		  
		  val aspenModel = ctct.getModel
      
      val categoricalFeature = ctct.categoricalFeature
				  
				  if(aspenModel.getType.equals(KMeansPredictionModel.spark_kmeans_prediction)) {
					  
					  return
							  
				  } else {
					  
					  val trainRDD = getDataset(ctct.datasetName)       
							  
							  //gather target categories
            
						val categoriesRDD = trainRDD.map { pair =>
						 val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
							  
						 val vitalBlock = new VitalBlock(inputObjects)
							  
						 val ex = aspenModel.getFeatureExtraction
							  
						 val featuresMap = ex.extractFeatures(vitalBlock)

              val category = featuresMap.get(categoricalFeature.getName)
                
              if(category == null) throw new RuntimeException("No category returned: " + pair._1)
            
              val c = category.asInstanceOf[VITAL_Category]
            
              (c.getURI, c)
            }
                
            val categories = categoriesRDD.reduceByKey( (c1: VITAL_Category , c2: VITAL_Category) => c1 ).map(p => p._2).collect()
            
            println("categories count: " + categories.size)
                
            val taxonomy = new Taxonomy()
            
            val rootCategory = new VITAL_Category()
            rootCategory.setURI("urn:taxonomy-root")
            rootCategory.setProperty("name", "Taxonomy Root")
            
            var container = new VITAL_Container()
            container.putGraphObject(rootCategory)
            
            taxonomy.setRootCategory(rootCategory)
            taxonomy.setRoot(rootCategory.getURI)
            taxonomy.setContainer(container)
            
            var c = 0
            for(x <- categories) {
              c = c+1
              container.putGraphObject(x)
              val edge = new Edge_hasChildCategory()
              edge.addSource(rootCategory).addDestination(x).setURI("urn:Edge_hasChildCategory_" + rootCategory.getURI + "_" + c)
              container.putGraphObject(edge)
            }
            
            val trainedCategories = CategoricalFeatureData.fromTaxonomy(taxonomy)

            aspenModel.getFeaturesData.put(categoricalFeature.getName, trainedCategories)
            
            globalContext.put(categoricalFeature.getName + CollectCategoricalFeatureTaxonomyDataTask.CATEGORICAL_FEATURE_DATA_SUFFIX, trainedCategories)
        }
		  
  }
  
  def checkDependencies_collectTextFeatureData(sc : SparkContext, ctfdt : CollectTextFeatureDataTask) : Unit = {
    
		  val trainRDD = getDataset(ctfdt.datasetName)
      
  }
  
  def collectTextFeatureData(sc : SparkContext, ctfdt : CollectTextFeatureDataTask) : Unit = {
 
    val trainRDD = getDataset(ctfdt.datasetName)  
    
    val feature = ctfdt.feature
    
    val minDF = feature.getMinDF
    
    val totalDocs = globalContext.get(ctfdt.datasetName + CountDatasetTask.DOCS_COUNT_SUFFIX)
    
    val maxDF = ( ( feature.getMaxDFP * totalDocs.asInstanceOf[Int] ) / 100f ).toInt
    
    val aspenModel = ctfdt.getModel
    
    val wordsRDD: RDD[String] = trainRDD.flatMap { pair =>

          val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
              
          val vitalBlock = new VitalBlock(inputObjects)
            
          var l = new HashSet[String]()
  
          val ex = aspenModel.getFeatureExtraction
          
          val featuresMap = ex.extractFeatures(vitalBlock)
          
          val textObj = featuresMap.get(feature.getName)
          
          if(textObj != null) {
            
            for (x <- textObj.asInstanceOf[String].toLowerCase().split("\\s+") ) {
              if (x.isEmpty()) {
                
              } else {
                l.add(x)
              }
            }
          }
      
  
          l.toSeq

        }
        
        val wordsOccurences = wordsRDD.map(x => (x, new Integer(1))).reduceByKey((i1, i2) => i1 + i2).filter(wordc => wordc._2 >= minDF && wordc._2 <= maxDF)

        val dict = Dictionary.createDictionary(mapAsJavaMap( wordsOccurences.collectAsMap() ) , minDF, maxDF) 
    
        val tfd = new TextFeatureData()
        tfd.setDictionary(dict)
        
        aspenModel.getFeaturesData.put(ctfdt.feature.getName, tfd)
        globalContext.put(ctfdt.feature.getName + CollectTextFeatureDataTask.TEXT_FEATURE_DATA_SUFFIX, tfd)
  }
  
  
  def checkDependencies_loadDataset(sc: SparkContext, ldt : LoadDataSetTask) : Unit= {
    
    if(isNamedRDDSupported()) {
//   		if( this.namedRdds.get(ldt.datasetName).isDefined ) throw new RuntimeException("Dataset already loaded: " + ldt.datasetName)
   	} else {
//  		if( datasetsMap.get(ldt.datasetName) != null) throw new RuntimeException("Dataset already loaded: " + ldt.datasetName);
  	}
    
  }
  
  def loadDataset(sc: SparkContext, ldt : LoadDataSetTask, serviceProfile: String) : Unit= {

    val inputName = ldt.path
    
    var inputBlockRDD : RDD[(String, Array[Byte])] = null
    
    var inputRDDName : String = null
    if(inputName.startsWith("name:")) {
      inputRDDName = inputName.substring("name:".length())
    }

    val model = ldt.getModel
    
    if(inputRDDName == null) {
        
        println("input path: " + inputName)
        
        val inputPath = new Path(inputName)
        
        val inputFS = FileSystem.get(inputPath.toUri(), hadoopConfig)
        
        if (!inputFS.exists(inputPath) /*|| !inputFS.isDirectory(inputPath)*/) {
          throw new RuntimeException("Input train path does not exist " + /*or is not a directory*/ ": " + inputPath.toString())
        }
        
        val inputFileStatus = inputFS.getFileStatus(inputPath)
        
        if(inputName.endsWith(".vital") || inputName.endsWith(".vital.gz")) {
            
            if(!inputFileStatus.isFile()) {
              throw new RuntimeException("input path indicates a block file but does not denote a file: " + inputName)
            }
            throw new RuntimeException("Vital block files not supported yet")
            
        } else {
          
          inputBlockRDD = sc.sequenceFile(inputPath.toString(), classOf[Text], classOf[VitalBytesWritable]).map { pair =>
            
            //make sure the URIReferences are resolved
            val inputObjects = VitalSigns.get().decodeBlock(pair._2.get, 0, pair._2.get.length)
            val vitalBlock = new VitalBlock(inputObjects)
            
            if(vitalBlock.getMainObject.isInstanceOf[URIReference]) {
              
              if( VitalSigns.get.getVitalService == null ) {
                if(serviceProfile != null) VitalServiceFactory.setServiceProfile(serviceProfile)
                VitalSigns.get.setVitalService(VitalServiceFactory.getVitalService)
              }
              
              //loads objects from features queries and train queries 
              model.getFeatureExtraction.composeBlock(vitalBlock)
              
              (pair._1.toString(), VitalSigns.get.encodeBlock(vitalBlock.toList()))
              
            } else {
              
            	(pair._1.toString(), pair._2.get)
            }

            
          }
         
          inputBlockRDD.cache()
          
        }
        
    } else {
      
      inputBlockRDD = this.namedRdds.get[(String, Array[Byte])](inputRDDName).get
      
      //quick scan to make sure the blocks are fetched
      inputBlockRDD = inputBlockRDD.map { pair =>
        
        
        //make sure the URIReferences are resolved
        val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
        val vitalBlock = new VitalBlock(inputObjects)
            
        if(vitalBlock.getMainObject.isInstanceOf[URIReference]) {
          
        	if( VitalSigns.get.getVitalService == null ) {
        		if(serviceProfile != null) VitalServiceFactory.setServiceProfile(serviceProfile)
        		VitalSigns.get.setVitalService(VitalServiceFactory.getVitalService)
        	}
              
          //loads objects from features queries and train queries 
          model.getFeatureExtraction.composeBlock(vitalBlock)
              
          (pair._1.toString(), VitalSigns.get.encodeBlock(vitalBlock.toList()))
              
        } else {
              
          pair
              
        }
        
      }
      
    }
    
    if(isNamedRDDSupported()) {
    	this.namedRdds.update(ldt.datasetName, inputBlockRDD);
    } else {
      datasetsMap.put(ldt.datasetName, inputBlockRDD)
    }
    
    globalContext.put(ldt.datasetName, inputBlockRDD)
    
  }
  
  def checkDependencies_countDataset(sc: SparkContext, cdt: CountDatasetTask) : Unit= {
    
		  val trainRDD = getDataset(cdt.datasetName)
      
  }
  def countDataset(sc: SparkContext, cdt: CountDatasetTask) : Unit= {
 
    val trainRDD = getDataset(cdt.datasetName)
    val x = trainRDD.count().intValue();
    
    globalContext.put(cdt.datasetName + CountDatasetTask.DOCS_COUNT_SUFFIX, x)
    
  }
 
  def checkDependencies_trainModel(sc: SparkContext, tmt : TrainModelTask) : Unit = {
		  val trainRDD = getDataset(tmt.datasetName)
  }
  
  def trainModel(sc: SparkContext, tmt : TrainModelTask) : Unit = {
 
		  val trainRDD = getDataset(tmt.datasetName)
      
      val aspenModel = tmt.getModel
      
      var outputModel : Serializable = null;
      
      if( CollaborativeFilteringPredictionModel.spark_collaborative_filtering_prediction.equals(aspenModel.getType)) {
      
        //first pass to collect user and products ids -> uris
        
        val cfpm = aspenModel.asInstanceOf[CollaborativeFilteringPredictionModel]
        
        val values = collaborativeFilteringCollectData(trainRDD, cfpm)
        
        val ac = aspenModel.getModelConfig.getAlgorithmConfig
        // Build the recommendation model using ALS
        var rank = 10
        val rankVal = ac.get("rank")
        if( rankVal != null && rankVal.isInstanceOf[Number]) {
          rank = rankVal.asInstanceOf[Number].intValue() 
        }
        
        var lambda = 0.01d
        val lambdaVal = ac.get("lambda")
        if(lambdaVal != null && lambdaVal.isInstanceOf[Number]) {
          lambda = lambdaVal.asInstanceOf[Number].doubleValue()
        }
        
        var iterations = 20
        val iterationsVal = ac.get("iterations")
        if(iterationsVal != null && iterationsVal.isInstanceOf[Number]) {
          iterations = iterationsVal.asInstanceOf[Number].intValue()
        }
        
        val ratings = values.map(quad => {
          new Rating(cfpm.getUserURI2ID().get(quad._1), cfpm.getProductURI2ID().get(quad._2), quad._3)
        })
        
        ratings.cache()
        
        globalContext.put("collaborative-filtering-ratings", ratings)
        
        val model = ALS.train(ratings, rank, iterations, lambda)
        
        cfpm.setModel(model)
        
        outputModel = model
        
        val usersProducts = values.map( triple => {
          (cfpm.getUserURI2ID().get(triple._1).toInt, cfpm.getProductURI2ID().get(triple._2).toInt )
        }) 
          
        val predictions = cfpm.getModel().predict(usersProducts).map { case Rating(user, product, rate) => 
          ((user, product), rate)
        }
          
        val ratesAndPreds = ratings.map { case Rating(user, product, rate) => 
            ((user, product), rate)
        }.join(predictions)
        val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) => 
        val err = (r1 - r2)
          err * err
        }.mean()
          
        val msg = "Mean Squared Error = " + MSE
          
        println(msg)
        cfpm.setError(msg)
        
//        val vectorized = vectorizeCollaborativeFiltering(trainRDD, aspenModel.asInstanceOf[CollaborativeFilteringPredictionModel])    
        
      } else if( DecisionTreePredictionModel.spark_decision_tree_prediction.equals(aspenModel.getType)) {
    
          val vectorized = vectorize(trainRDD, aspenModel);
          
          val numClasses = aspenModel.getTrainedCategories.getCategories.size()
          val categoricalFeaturesInfo = aspenModel.asInstanceOf[PredictionModel].getCategoricalFeaturesMap()
          val impurity = "gini"
          val maxDepth = 5
          val maxBins = 100
          
          val model = DecisionTree.trainClassifier(vectorized, numClasses, categoricalFeaturesInfo.toMap, impurity,
            maxDepth, maxBins)
        
          aspenModel.asInstanceOf[DecisionTreePredictionModel].setModel(model)
          
          outputModel = model
          
        } else if( KMeansPredictionModel.spark_kmeans_prediction.equals(aspenModel.getType)) {
          
          val parsedData = vectorizeNoLabels(trainRDD, aspenModel)
          
          var clustersCount = 10
          var clustersCountSetting = aspenModel.getModelConfig.getAlgorithmConfig.get("clustersCount");
          if(clustersCountSetting != null && clustersCountSetting.isInstanceOf[Number]) {
            clustersCount = clustersCountSetting.asInstanceOf[Number].intValue()
          }
          
          
          // Cluster the data into two classes using KMeans
          var numIterations = 20
          var numIterationsSetting = aspenModel.getModelConfig.getAlgorithmConfig.get("numIterations")
          if(numIterationsSetting != null && numIterationsSetting.isInstanceOf[Number]) {
            numIterations = numIterationsSetting.asInstanceOf[Number].intValue()
          }
          val clusters = KMeans.train(parsedData, clustersCount, numIterations)
          
          
          aspenModel.asInstanceOf[KMeansPredictionModel].setModel(clusters)
          
          // Evaluate clustering by computing Within Set Sum of Squared Errors
          val WSSSE = clusters.computeCost(parsedData)
          val msg = "Within Set Sum of Squared Errors = " + WSSSE
    
          println(msg)
          aspenModel.asInstanceOf[KMeansPredictionModel].setError(msg)
          
          outputModel = clusters
          
        } else if( NaiveBayesPredictionModel.spark_naive_bayes_prediction.equals(aspenModel.getType)) {
          
          val vectorized = vectorize(trainRDD, aspenModel);
          
          val model = NaiveBayes.train(vectorized, lambda = 1.0)
          
          aspenModel.asInstanceOf[NaiveBayesPredictionModel].setModel(model)
          
          outputModel = model
          
        } else if( RandomForestPredictionModel.spark_randomforest_prediction.equals(aspenModel.getType ) ) {
          
          val vectorized = vectorize(trainRDD, aspenModel);
          
          val numClasses = aspenModel.getTrainedCategories.getCategories.size()
          val categoricalFeaturesInfo = aspenModel.asInstanceOf[PredictionModel].getCategoricalFeaturesMap()
          val numTrees = 20 // Use more in practice.
          val featureSubsetStrategy = "auto" // Let the algorithm choose.
          val impurity = "gini"
          val maxDepth = 20
          val maxBins = 32
          
          val model = RandomForest.trainClassifier(vectorized, numClasses, categoricalFeaturesInfo.toMap,
              numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
              
          aspenModel.asInstanceOf[RandomForestPredictionModel].setModel(model)
          
          outputModel = model
          
        } else if(RandomForestRegressionModel.spark_randomforest_regression.equals(aspenModel.getType)) {
          
          val vectorized = vectorize(trainRDD, aspenModel);
          
          val categoricalFeaturesInfo = aspenModel.asInstanceOf[PredictionModel].getCategoricalFeaturesMap()
          val numTrees = 3 // Use more in practice.
          val featureSubsetStrategy = "auto" // Let the algorithm choose.
          val impurity = "variance"
          val maxDepth = 4
          val maxBins = 32

          val model = RandomForest.trainRegressor(vectorized, categoricalFeaturesInfo.toMap,
              numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
              
          aspenModel.asInstanceOf[RandomForestRegressionModel].setModel(model)
          
          outputModel = model
          
        } else if(SparkLinearRegressionModel.spark_linear_regression.equals(aspenModel.getType)) {
          
          val vectorized = vectorizeWithScaling(trainRDD, aspenModel.asInstanceOf[SparkLinearRegressionModel]);
          
//          val scaler = new StandardScaler(true, true).fit(vectorized.)
          
          var numIterations = 10
          var numIterationsSetting = aspenModel.getModelConfig.getAlgorithmConfig.get("numIterations")
          if(numIterationsSetting != null && numIterationsSetting.isInstanceOf[Number]) {
            numIterations = numIterationsSetting.asInstanceOf[Number].intValue()
          }
          
          
          var c = 0
          val x = vectorized.collect()
          while ( c < 10 ) {
            println ("Point: " + x(c))
            c = c+1
          }
          
          //normalize

          val model = LinearRegressionWithSGD.train(vectorized, numIterations)
          
          aspenModel.asInstanceOf[SparkLinearRegressionModel].setModel(model)
          
          outputModel = model
          
        } else {
          throw new RuntimeException("Unhandled aspen model type: " + aspenModel.getType)
        } 
      
      globalContext.put(TrainModelTask.MODEL_BINARY, outputModel)
    
  }
  
  def checkDependencies_splitDataset(sc: SparkContext, sdt : SplitDatasetTask) : Unit = {
    
    getDataset(sdt.inputDatasetName)
    
  }
  
  def splitDataset(sc: SparkContext, sdt : SplitDatasetTask) : Unit = {
		  
		  val inputRDD = getDataset(sdt.inputDatasetName)
      
      val splits = inputRDD.randomSplit(Array(sdt.firstSplitRatio, 1 - sdt.firstSplitRatio), seed = 11L)
      
      if(isNamedRDDSupported()) {
        
        this.namedRdds.update(sdt.outputDatasetName1, splits(0))
        this.namedRdds.update(sdt.outputDatasetName2, splits(1))
        
      } else {
        
        datasetsMap.put(sdt.outputDatasetName1, splits(0))
        datasetsMap.put(sdt.outputDatasetName2, splits(1))
        
      }
      
      globalContext.put(sdt.outputDatasetName1, splits(0))
      globalContext.put(sdt.outputDatasetName2, splits(1))
      
  }
  
  def checkDependencies_testModel(sc: SparkContext, tmt : TestModelTask) : Unit = {
 
    val testRDD = getDataset(tmt.datasetName)
  }
  
  def testModel(sc: SparkContext, tmt : TestModelTask) : Unit = {
 
    val aspenModel = tmt.getModel
    
    val testRDD = getDataset(tmt.datasetName)
    
        if(CollaborativeFilteringPredictionModel.spark_collaborative_filtering_prediction.equals(aspenModel.getType)) {
          
          val cfpm = aspenModel.asInstanceOf[CollaborativeFilteringPredictionModel]
          
          val values = globalContext.get("collaborative-filtering-rdd").asInstanceOf[RDD[(String, String, Double)]] 
          
          val usersProducts = values.map( triple => {
            (cfpm.getUserURI2ID().get(triple._1).toInt, cfpm.getProductURI2ID().get(triple._2).toInt )
          }) 
          
          val predictions = cfpm.getModel().predict(usersProducts).map { case Rating(user, product, rate) => 
            ((user, product), rate)
          }
          
          val ratings = globalContext.get("collaborative-filtering-ratings").asInstanceOf[RDD[Rating]]
          val ratesAndPreds = ratings.map { case Rating(user, product, rate) => 
            ((user, product), rate)
          }.join(predictions)
          val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) => 
          val err = (r1 - r2)
            err * err
          }.mean()
          
          val msg = "Mean Squared Error = " + MSE
          
          println(msg)
          cfpm.setError(msg)
          
        } else if( DecisionTreePredictionModel.spark_decision_tree_prediction.equals(aspenModel.getType)) {
    
          println("Testing ...")
          
          println("Test documents count: " + testRDD.count())
          
          val vectorizedTest = vectorize(testRDD, aspenModel)
          
          val labelAndPreds = vectorizedTest.map { point =>
          val prediction = aspenModel.asInstanceOf[DecisionTreePredictionModel].getModel().predict(point.features)
            (point.label, prediction)
          }
          
          val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / vectorizedTest.count()
          
          val msg = "Test Error = " + testErr
          println(msg)
          aspenModel.asInstanceOf[DecisionTreePredictionModel].setError(msg)
          
        } else if( NaiveBayesPredictionModel.spark_naive_bayes_prediction.equals(aspenModel.getType)) {

          println("Testing ...")
          
          println("Test documents count: " + testRDD.count())
          
          val vectorizedTest = vectorize(testRDD, aspenModel)
          
          val predictionAndLabel = vectorizedTest.map(p => (aspenModel.asInstanceOf[NaiveBayesPredictionModel].getModel.predict(p.features), p.label))
          val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / vectorizedTest.count()
          
          val msg = "Accuracy: " + accuracy;
          
          println(msg)
          
          aspenModel.asInstanceOf[NaiveBayesPredictionModel].setError(msg);
          
        } else if( RandomForestPredictionModel.spark_randomforest_prediction.equals(aspenModel.getType ) ) {
          
          println("Testing ...")
          
          println("Test documents count: " + testRDD.count())
          
          val vectorizedTest = vectorize(testRDD, aspenModel)
          
          
    //    val predictionAndLabel = vectorizedTest.map(p => (model.predict(p.features), p.label))
    //    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / vectorizedTest.count()
    //
    //    println("Accuracy: " + accuracy)
          
          // Evaluate model on test instances and compute test error
          val labelAndPreds = vectorizedTest.map { point =>
            val prediction = aspenModel.asInstanceOf[RandomForestPredictionModel].getModel.predict(point.features)
            (point.label, prediction)
          }
          
          val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / vectorizedTest.count()
    //    println("Learned classification forest model:\n" + model.toDebugString)
              
          val msg = "Test Error = " + testErr
          println(msg)
          
          aspenModel.asInstanceOf[RandomForestPredictionModel].setError(msg)
          
        } else if( RandomForestRegressionModel.spark_randomforest_regression.equals(aspenModel.getType)) {
          
          val rfrm = aspenModel.asInstanceOf[RandomForestRegressionModel];
          
          // Evaluate model on training examples and compute training error
          val valuesAndPreds = vectorize(testRDD, aspenModel).map { point =>
            val prediction = rfrm.getModel().predict(point.features)
            (point.label, prediction)
          }
          
          val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
          var msg = "training Mean Squared Error = " + MSE
          msg = msg + "\nLearned regression forest model:\n" + rfrm.getModel().toDebugString
          println(msg)
          
          rfrm.setError(msg)
          
        } else if( SparkLinearRegressionModel.spark_linear_regression.equals(aspenModel.getType)) {
          
          val sprm = aspenModel.asInstanceOf[SparkLinearRegressionModel];
          
          // Evaluate model on training examples and compute training error
          val valuesAndPreds = vectorize(testRDD, aspenModel).map { point =>
            println ("Test Point: " + point)
            val scaledPoint = sprm.scaleLabeledPoint(point)
            println ("Test Point scaled: " + scaledPoint)
            val prediction = sprm.getModel().predict(scaledPoint.features)
            println("Prediction: " + prediction)
            var original = sprm.scaledBack(prediction);
            println("Input: " + prediction + " rescaled: " + original)
            (point.label, original)
          }
          
          val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
          val msg = "training Mean Squared Error = " + MSE
          
          println(msg)
          
          sprm.setError(msg)
          
        } else {
          throw new RuntimeException("Unhandled model testing: " + aspenModel.getType)
        } 
  }
 
  def checkDependencies_collectNumericalFeatureDataTask(sc : SparkContext, cnfdt: CollectNumericalFeatureDataTask) : Unit = {
    
  }
  
  def collectNumericalFeatureDataTask(sc : SparkContext, cnfdt: CollectNumericalFeatureDataTask) : Unit = {
  
    if( cnfdt.getModel.getFeaturesData.get(cnfdt.feature.getName) == null ) {
      
    	cnfdt.getModel.getFeaturesData.put(cnfdt.feature.getName, new NumericalFeatureData())
      
    }
    
    globalContext.put(cnfdt.feature.getName + CollectNumericalFeatureDataTask.NUMERICAL_FEATURE_DATA_SUFFIX, cnfdt.getModel.getFeaturesData.get(cnfdt.feature.getName))
    
  }
  
  def collaborativeFilteringCollectData(trainRDD : RDD[(String, Array[Byte])], model : CollaborativeFilteringPredictionModel) : RDD[(String, String, Double)] = {
    
    val values = trainRDD.map( pair => {
 
      val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
      
      val fe = model.getFeatureExtraction 
      
      val features = fe.extractFeatures(new VitalBlock(inputObjects))
      
      val userURI = features.get(CollaborativeFilteringPredictionModel.feature_user_uri)
      if(userURI == null) throw new RuntimeException("No " + CollaborativeFilteringPredictionModel.feature_user_uri)
      if(!userURI.isInstanceOf[String]) throw new RuntimeException("Feature " + CollaborativeFilteringPredictionModel.feature_user_uri + " must be a string")
      
      val productURI = features.get(CollaborativeFilteringPredictionModel.feature_product_uri)
      if(productURI == null) throw new RuntimeException("No " + CollaborativeFilteringPredictionModel.feature_product_uri)
      if(!productURI.isInstanceOf[String]) throw new RuntimeException("Feature " + CollaborativeFilteringPredictionModel.feature_product_uri + " must be a string")      
      
      val rating = features.get(CollaborativeFilteringPredictionModel.feature_rating)
      if(rating == null) throw new RuntimeException("No " + CollaborativeFilteringPredictionModel.feature_rating)
      if(!rating.isInstanceOf[Number]) throw new RuntimeException("Feature " + CollaborativeFilteringPredictionModel.feature_rating + " must be a number (double)")
      
      (userURI.asInstanceOf[String], productURI.asInstanceOf[String], rating.asInstanceOf[Number].doubleValue())
      
    })
    
    values.cache()
    
    val usersList = values.map( triple => {
      (triple._1)
    }).distinct().collect()
    
    val userURI2IDdic = new HashMap[String, Integer]

    var c = 0
    for(u <- usersList) {
      userURI2IDdic.put(u, c)
      c = c+1
    }
    
    val productsList = values.map ( triple => {
      (triple._2)
    }).distinct().collect()
    
    c = 0
    val productURI2IDdic = new HashMap[String, Integer]
    for(p <- productsList) {
      productURI2IDdic.put(p, c)
      c = c+1
    }
    
    model.setUserURI2ID(new Dictionary(userURI2IDdic))
    model.setProductURI2ID(new Dictionary(productURI2IDdic))
    
    globalContext.put("collaborative-filtering-rdd", values)
    values
    
    
  }
  
}