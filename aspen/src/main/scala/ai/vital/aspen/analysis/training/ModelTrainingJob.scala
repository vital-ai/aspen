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
import ai.vital.aspen.groovy.predict.tasks.CollectCategoricalFeaturesDataTask
import ai.vital.aspen.groovy.predict.tasks.CollectNumericalFeatureDataTask
import ai.vital.aspen.groovy.predict.tasks.CollectTargetCategoriesTask
import ai.vital.aspen.groovy.predict.tasks.CollectTextFeatureDataTask
import ai.vital.aspen.groovy.predict.tasks.CountDatasetTask
import ai.vital.aspen.groovy.predict.tasks.LoadDataSetTask
import ai.vital.aspen.groovy.predict.tasks.ProvideMinDFMaxDF
import ai.vital.aspen.groovy.predict.tasks.SaveModelTask
import ai.vital.aspen.groovy.predict.tasks.SplitDatasetTask
import ai.vital.aspen.groovy.predict.tasks.TrainModelTask
import ai.vital.aspen.groovy.predict.tasks.TestModelTask
import ai.vital.predictmodel.Feature
import ai.vital.aspen.model.PredictionModel
import ai.vital.aspen.util.SetOnceHashMap

class ModelTrainingJob {}

object ModelTrainingJob extends AbstractJob {
  
  val modelBuilderOption = new Option("b", "model-builder", true, "model builder file")
  
  modelBuilderOption.setRequired(true)
  
  //expects 20news messages
  val inputOption  = new Option("i", "input", true, "input RDD[(String, Array[Byte])], either named RDD name (name:<name>) or <path> (no prefix), where path is a .vital.seq or .vital file")
  inputOption.setRequired(true)
  
  val outputOption = new Option("mod", "model", true, "output model path (directory)")
  outputOption.setRequired(true)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite model if exists")
  overwriteOption.setRequired(false)
  
  val minDFOption = new Option("minDF", "minimumDocFreq", true, "minimum term document frequency, default: " + MIN_DF)
  minDFOption.setRequired(false)
  
  val maxDFPercentOption = new Option("maxDFP", "maxDocFreqPercent", true, "maximum term document frequency (percent), default: " + MAX_DF_PERCENT)
  maxDFPercentOption.setRequired(false)
  
  def MIN_DF = 1
  
  def MAX_DF_PERCENT = 100
  
  //this is only used when namedRDDs support is disabled
  var datasetsMap : java.util.HashMap[String, RDD[(String, Array[Byte])]] = null;
  
  var hadoopConfig : Configuration = null
  
  val globalContext = new SetOnceHashMap()
  
  def getOptions(): Options = {
    addJobServerOptions(
      new Options()
      .addOption(masterOption)
      .addOption(modelBuilderOption)
      .addOption(inputOption)
      .addOption(outputOption)
      .addOption(overwriteOption)
      .addOption(minDFOption)
      .addOption(maxDFPercentOption)
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
		  
    val inputName = jobConfig.getString(inputOption.getLongOpt)
    
    var inputRDDName : String = null
    if(inputName.startsWith("name:")) {
      inputRDDName = inputName.substring("name:".length())
    }
    
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
    
    var minDF = MIN_DF
    var maxDFPercent = MAX_DF_PERCENT
    
    try {
        minDF = Integer.parseInt(jobConfig.getString(minDFOption.getLongOpt))
    } catch {
      case ex: ConfigException.Missing => {}
    }
    
    try {
      maxDFPercent = Integer.parseInt(jobConfig.getString(maxDFPercentOption.getLongOpt))
    } catch {
      case ex: ConfigException.Missing => {}
    }
    
    if(minDF < 1) {
      throw new RuntimeException("minDF must be > 0")
    }
    
    if(maxDFPercent > 100 || maxDFPercent < 1) {
      throw new RuntimeException("maxDFPercent must be within range [1, 100]")
    }
    
    
    
    println("input train name: " + inputName)
    println("builder path: " + builderPath)
    println("output model path: " + outputModelPath)
    if(zipContainer) println("   output is a zip container (.zip)")
    if(jarContainer) println("   output is a jar container (.jar)")
    println("overwrite if exists: " + overwrite)
    println("service profile: " + serviceProfile)
    println("minDF: " + minDF)
    println("maxDFPercent: " + maxDFPercent)
    
    
    if(isNamedRDDSupported()) {
    	println("named RDDs supported")
    } else {
    	println("named RDDs not supported, storing RDD references in a local cache")
    	datasetsMap = new java.util.HashMap[String, RDD[(String, Array[Byte])]]();
    }
    
    val creatorMap = new HashMap[String, Class[_ <: AspenModel]];
    creatorMap.put(DecisionTreePredictionModel.spark_decision_tree_prediction, classOf[DecisionTreePredictionModel]);
    creatorMap.put(KMeansPredictionModel.spark_kmeans_prediction, classOf[KMeansPredictionModel]);
    creatorMap.put(NaiveBayesPredictionModel.spark_naive_bayes_prediction, classOf[NaiveBayesPredictionModel]);
    creatorMap.put(RandomForestPredictionModel.spark_randomforest_prediction, classOf[RandomForestPredictionModel])
    val modelCreator = new ModelCreator(creatorMap)
    
    hadoopConfig = new Configuration()
    
    
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
    
    if( !creatorMap.containsKey( aspenModel.getType ) ) {
      throw new RuntimeException("only the following model types are supported: " + creatorMap.keySet().toString())
    }

    
    val segmentsList = new ArrayList[VitalSegment]()
    
      
    if(serviceProfile != null) {
        VitalServiceFactory.setServiceProfile(serviceProfile)
    }

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
        
      } else if(task.isInstanceOf[CollectCategoricalFeaturesDataTask]) {
        
        val ccfdt = task.asInstanceOf[CollectCategoricalFeaturesDataTask]
        
        checkDependencies_collectCategoricalFeatureDataTask(sc, ccfdt)
        
        collectCategoricalFeatureDataTask(sc, ccfdt)
        
      } else if(task.isInstanceOf[CollectNumericalFeatureDataTask]) {
        
        val cnfdt = task.asInstanceOf[CollectNumericalFeatureDataTask]
        
        checkDependencies_collectNumericalFeatureDataTask(sc, cnfdt)
        
        collectNumericalFeatureDataTask(sc, cnfdt)
        
      } else if(task.isInstanceOf[CollectTargetCategoriesTask]) {
        
    	  val ctct = task.asInstanceOf[CollectTargetCategoriesTask]

        checkDependencies_collectTargetCategories(sc, ctct)
          
        collectTargetCategories(sc, ctct)
          
      } else if(task.isInstanceOf[CollectTextFeatureDataTask]) {
        
        val ctfdt = task.asInstanceOf[CollectTextFeatureDataTask]
        
        checkDependencies_collectTextFeatureData(sc, ctfdt)
        
        collectTextFeatureData(sc, ctfdt)
        
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
        
        loadDataset(sc, ldt)
        
      } else if(task.isInstanceOf[ProvideMinDFMaxDF]) {
        
        
        val pmm = task.asInstanceOf[ProvideMinDFMaxDF]
        
        checkDependencies_provideMinDFMaxDF(sc, pmm)
        
        provideMinDFMaxDF(sc, pmm, minDF, maxDFPercent)
        
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
    
    

    
    println("DONE " + new Date().toString())
		  
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
      
      val ex = new FeatureExtraction(model.getModelConfig, model.getAggregationResults)
      
      val featuresMap = ex.extractFeatures(vitalBlock)

      model.asInstanceOf[ai.vital.aspen.model.PredictionModel].vectorizeNoLabels(vitalBlock, featuresMap);
      
    }
    
    return vectorized
    
  }
  
  
  def vectorize (trainRDD: RDD[(String, Array[Byte])], model: AspenModel) : RDD[LabeledPoint] = {

    val vectorized = trainRDD.map { pair =>
      
      val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
            
      val vitalBlock = new VitalBlock(inputObjects)
      
      val ex = new FeatureExtraction(model.getModelConfig, model.getAggregationResults)
      
      val featuresMap = ex.extractFeatures(vitalBlock)

      model.asInstanceOf[ai.vital.aspen.model.PredictionModel].vectorizeLabels(vitalBlock, featuresMap);
      
    }
    
    return vectorized
    
  }
  
  override def subvalidate(sc: SparkContext, config: Config) : SparkJobValidation = {
    
    val inputValue = config.getString(inputOption.getLongOpt)
    
    if(!skipNamedRDDValidation && inputValue.startsWith("name:")) {
      
      val inputRDDName = inputValue.substring("name:".length)
      
      if(!isNamedRDDSupported()) {
    	  return new SparkJobInvalid("Cannot use named RDD output - no spark job context")
      }
      
      val inputRDD = this.namedRdds.get[(String, Array[Byte])](inputRDDName)
//      val rdd = this.namedRdds.get[(Long, scala.Seq[String])]("dictionary")
      
      if( !inputRDD.isDefined ) SparkJobInvalid("Missing named RDD [" + inputRDDName + "]")
        
      
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
          
          val ex = new FeatureExtraction(aspenModel.getModelConfig, aspenModel.getAggregationResults);
              
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
  
  def checkDependencies_collectTargetCategories(sc: SparkContext, ctct : CollectTargetCategoriesTask) : Unit = {
    val trainRDD = getDataset(ctct.datasetName)       
  }
  
  def collectTargetCategories(sc: SparkContext, ctct : CollectTargetCategoriesTask) : Unit = {
 
    val aspenModel = ctct.getModel
    
    if(aspenModel.getType.equals(KMeansPredictionModel.spark_kmeans_prediction)) {
          
      return
          
    } else {
    
        val trainRDD = getDataset(ctct.datasetName)       
      
          //gather target categories
          val categoriesRDD = trainRDD.map { pair =>
            val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
            
            val vitalBlock = new VitalBlock(inputObjects)
            
            val ex = new FeatureExtraction(aspenModel.getModelConfig, aspenModel.getAggregationResults);
            
            val featuresMap = ex.extractFeatures(vitalBlock)
                
            val category = aspenModel.getModelConfig.getTrain.call(vitalBlock, featuresMap)
                
            if(category == null) throw new RuntimeException("No category returned: " + pair._1)
            
            category.asInstanceOf[String]
                
          }
          
          val categories: Array[String] = categoriesRDD.distinct().toArray().sortWith((s1, s2) => s1.compareTo(s2) < 0)
              
          println("categories count: " + categories.size)
          
          val trainedCategories = new CategoricalFeatureData()
          trainedCategories.setCategories(categories.toList)
          aspenModel.setTrainedCategories(trainedCategories)
              
          globalContext.put(CollectTargetCategoriesTask.TARGET_CATEGORIES_DATA, categories)
    }
    
  }
  
  def checkDependencies_collectTextFeatureData(sc : SparkContext, ctfdt : CollectTextFeatureDataTask) : Unit = {
    
		  val trainRDD = getDataset(ctfdt.datasetName)
      
  }
  
  def collectTextFeatureData(sc : SparkContext, ctfdt : CollectTextFeatureDataTask) : Unit = {
 
    val trainRDD = getDataset(ctfdt.datasetName)  
    
    val feature = ctfdt.feature
    
    val minDF = globalContext.get(ctfdt.datasetName + ProvideMinDFMaxDF.MIN_DF_SUFFIX).asInstanceOf[Int]
    val maxDF = globalContext.get(ctfdt.datasetName + ProvideMinDFMaxDF.MAX_DF_SUFFIX).asInstanceOf[Int]
    
    val aspenModel = ctfdt.getModel
    
    val wordsRDD: RDD[String] = trainRDD.flatMap { pair =>

          val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
              
          val vitalBlock = new VitalBlock(inputObjects)
            
          var l = new HashSet[String]()
  
          val ex = new FeatureExtraction(aspenModel.getModelConfig, aspenModel.getAggregationResults);
          
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
  
  def checkDependencies_provideMinDFMaxDF(sc: SparkContext, pmm : ProvideMinDFMaxDF) : Unit= {
    
  }
  
  def provideMinDFMaxDF(sc: SparkContext, pmm : ProvideMinDFMaxDF, minDF: Int, maxDFPercent : Int) : Unit= {
    
    val totalDocs = globalContext.get(pmm.datasetName + CountDatasetTask.DOCS_COUNT_SUFFIX).asInstanceOf[Integer]
    
    var maxDF : Int = totalDocs * maxDFPercent / 100

    globalContext.put(pmm.datasetName + ProvideMinDFMaxDF.MIN_DF_SUFFIX, minDF)
    globalContext.put(pmm.datasetName + ProvideMinDFMaxDF.MAX_DF_SUFFIX, maxDF)

  }
  
  def checkDependencies_loadDataset(sc: SparkContext, ldt : LoadDataSetTask) : Unit= {
    
    if(isNamedRDDSupported()) {
//   		if( this.namedRdds.get(ldt.datasetName).isDefined ) throw new RuntimeException("Dataset already loaded: " + ldt.datasetName)
   	} else {
//  		if( datasetsMap.get(ldt.datasetName) != null) throw new RuntimeException("Dataset already loaded: " + ldt.datasetName);
  	}
    
  }
  
  def loadDataset(sc: SparkContext, ldt : LoadDataSetTask) : Unit= {

    val inputName = ldt.path
    
    var inputBlockRDD : RDD[(String, Array[Byte])] = null
    
    var inputRDDName : String = null
    if(inputName.startsWith("name:")) {
      inputRDDName = inputName.substring("name:".length())
    }

    
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
            (pair._1.toString(), pair._2.get)
          }
         
          inputBlockRDD.cache()
          
        }
        
    } else {
      
      inputBlockRDD = this.namedRdds.get[(String, Array[Byte])](inputRDDName).get
      
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
      
      if( DecisionTreePredictionModel.spark_decision_tree_prediction.equals(aspenModel.getType)) {
    
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
    
        if( DecisionTreePredictionModel.spark_decision_tree_prediction.equals(aspenModel.getType)) {
    
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
          
        } else {
          throw new RuntimeException("Unhandled model testing: " + aspenModel.getType)
        } 
  }
 
  def checkDependencies_collectCategoricalFeatureDataTask(sc : SparkContext, ccfdt: CollectCategoricalFeaturesDataTask) : Unit = {
    
  }
  def collectCategoricalFeatureDataTask(sc : SparkContext, ccfdt: CollectCategoricalFeaturesDataTask) : Unit = {
    
    val aspenModel = ccfdt.getModel
    
    if(!aspenModel.getFeaturesData.containsKey(ccfdt.feature.getName)) {
    	//TODO
      val cfd = new CategoricalFeatureData()
    	ccfdt.getModel.getFeaturesData.put(ccfdt.feature.getName, cfd);
    }
    
    globalContext.put(ccfdt.feature.getName + CollectCategoricalFeaturesDataTask.CATEGORICAL_FEATURE_DATA_SUFFIX, aspenModel.getFeaturesData.get(ccfdt.feature.getName))
    
  }
  
  def checkDependencies_collectNumericalFeatureDataTask(sc : SparkContext, cnfdt: CollectNumericalFeatureDataTask) : Unit = {
    
  }
  
  def collectNumericalFeatureDataTask(sc : SparkContext, cnfdt: CollectNumericalFeatureDataTask) : Unit = {
  
    if( cnfdt.getModel.getFeaturesData.get(cnfdt.feature.getName) == null ) {
      
    	cnfdt.getModel.getFeaturesData.put(cnfdt.feature.getName, new NumericalFeatureData())
      
    }
    
    globalContext.put(cnfdt.feature.getName + CollectNumericalFeatureDataTask.NUMERICAL_FEATURE_DATA_SUFFIX, cnfdt.getModel.getFeaturesData.get(cnfdt.feature.getName))
    
  }
  
}