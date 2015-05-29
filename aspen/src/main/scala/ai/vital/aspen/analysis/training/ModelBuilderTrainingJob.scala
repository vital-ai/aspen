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

class ModelBuilderTrainingJob {}

object ModelBuilderTrainingJob extends AbstractJob {
  
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
    
    
    val creatorMap = new HashMap[String, Class[_ <: AspenModel]];
    creatorMap.put(DecisionTreePredictionModel.spark_decision_tree_prediction, classOf[DecisionTreePredictionModel]);
    creatorMap.put(KMeansPredictionModel.spark_kmeans_prediction, classOf[KMeansPredictionModel]);
    creatorMap.put(NaiveBayesPredictionModel.spark_naive_bayes_prediction, classOf[NaiveBayesPredictionModel]);
    creatorMap.put(RandomForestPredictionModel.spark_randomforest_prediction, classOf[RandomForestPredictionModel])
    val modelCreator = new ModelCreator(creatorMap)
    
    val hadoopConfig = new Configuration()
    
    
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

    
    val featuresMap = new Features().parseFeaturesMap(aspenModel)
    
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
    
    var inputBlockRDD : RDD[(String, Array[Byte])] = null
    
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

    
    var trainRDD : RDD[(String, Array[Byte])] = null
    
    var testRDD : RDD[(String, Array[Byte])] = null
    
    if(false) {
      
      //use full set
//      trainRDD = inputRDD
      
    } else {
      
      val splits = inputBlockRDD.randomSplit(Array(0.6, 0.4), seed = 11L)
          
      trainRDD = splits(0)
          
      testRDD = splits(1)
      
    }
    
    val docsCount = trainRDD.count();
    
    println("Documents count: " + docsCount)
    
    val maxDF = docsCount * maxDFPercent / 100
    
    println("MaxDF: " + maxDF)
    
    
    val modelCfg = aspenModel.getModelConfig
    
    //just collect a map of feature
    val aggregates = aspenModel.getModelConfig.getAggregates;
    
    //do a pass over data to collect aggregation values, introspect categorical values and generate dictionaries
    
    val aggregatesResults = new HashMap[String, java.lang.Double]();
    
    aspenModel.setAggregationResults(aggregatesResults);
    
    for(a<-aggregates) {
      
      var acc : Accumulator[Int] = null; 
      
      if(a.getFunction == Function.AVERAGE) {
        acc = sc.accumulator(0);
      }
      
      val numerics = trainRDD.map { pair =>
            
        val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
            
        val vitalBlock = new VitalBlock(inputObjects)
            
        val aggFunctions = PredictionModelAnalyzer.getAggregationFunctions(aspenModel.getModelConfig, a)
            
        val ex = new FeatureExtraction(aspenModel.getModelConfig, aggregatesResults);
            
        val features = ex.extractFeatures(vitalBlock, aggFunctions)
            
        val fv = features.get( a.getRequires().get(0) )
        
        if(fv == null) {
          return 0d
        }
        
        if( ! fv.isInstanceOf[Number] ) {
          throw new RuntimeException("Expected double value")
        }
        
        if(acc != null) {
          acc += 1
        }
        
        fv.asInstanceOf[Number].doubleValue()
        
            
      }
      
      if(a.getFunction == Function.AVERAGE) {
        
        if(acc.value == 0) {
        	aggregatesResults.put( a.getProvides, 0d);
        } else {
          
        	val reduced = numerics.reduce { (a1, a2) =>
        	a1 + a2
        	}
        	
        	aggregatesResults.put(a.getProvides, reduced /acc.value)
          
        } 
        
        
      } else if(a.getFunction == Function.SUM) {
  	    aggregatesResults.put(a.getProvides, numerics.sum())
      } else if(a.getFunction == Function.MIN) {
    	  aggregatesResults.put(a.getProvides, numerics.min())
      } else if(a.getFunction == Function.MAX) {
    	  aggregatesResults.put(a.getProvides, numerics.max())
      } else {
        throw new RuntimeException("Unhandled aggregation function: " + a.getFunction)
      }
      
//      inputBlockRDD.ma
      
    }
    
    //TODO categories introscpection / setting
    
    
    
    //gather target categories
    val categoriesRDD = trainRDD.map { pair =>
      val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
              
      val vitalBlock = new VitalBlock(inputObjects)
      
      val ex = new FeatureExtraction(aspenModel.getModelConfig, aggregatesResults);
          
      val featuresMap = ex.extractFeatures(vitalBlock)
          
      val category = aspenModel.getModelConfig.getTrain.call(vitalBlock, featuresMap)
      
      if(category == null) throw new RuntimeException("No category returned: " + pair._1)
      
      category.asInstanceOf[String]
      
    }
    
    val categories: Array[String] = categoriesRDD.toArray().sortWith((s1, s2) => s1.compareTo(s2) < 0)

    println("categories count: " + categories.size)
    
    val cfd = new CategoricalFeatureData()
    cfd.setCategories(categories.toList)
    aspenModel.setTrainedCategories(cfd)
    
    
    
    
    
    // calculate dictionaries
    for( f <- aspenModel.getModelConfig.getFeatures ) {
      
      if( f.isInstanceOf[TextFeature] ) {

        val wordsRDD: RDD[String] = trainRDD.flatMap { pair =>

          val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
              
          val vitalBlock = new VitalBlock(inputObjects)
            
          var l = new HashSet[String]()
  
          val ex = new FeatureExtraction(aspenModel.getModelConfig, aggregatesResults);
          
          val featuresMap = ex.extractFeatures(vitalBlock)
          
          val textObj = featuresMap.get(f.getName)
          
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

        val dict = Dictionary.createDictionary(mapAsJavaMap( wordsOccurences.collectAsMap() ) , minDF, maxDF.intValue()) 

        val tfd = new TextFeatureData()
        tfd.setDictionary(dict)
        
        aspenModel.getFeaturesData.put(f.getName, tfd);
        
      }
      
    }
    
    
    println("Training model...")
    
    val catMap = new HashMap[Int, Int]()
    val categoricalFeaturesTotal = 0/*allEntities.size + allSpanTypes.size*/
    
    var x = 0
    while ( x < categoricalFeaturesTotal) {
      catMap.put(x, 2)
      x = x + 1
    }

    if( DecisionTreePredictionModel.spark_decision_tree_prediction.equals(aspenModel.getType)) {

      val vectorized = vectorize(trainRDD, aspenModel);
      
      val numClasses = categories.length
      val categoricalFeaturesInfo = catMap.toMap
      val impurity = "gini"
      val maxDepth = 5
      val maxBins = 100
      
      val model = DecisionTree.trainClassifier(vectorized, numClasses, categoricalFeaturesInfo, impurity,
        maxDepth, maxBins)
    
      aspenModel.asInstanceOf[DecisionTreePredictionModel].setModel(model)
      
      println("Testing ...")
      
      println("Test documents count: " + testRDD.count())
      
      val vectorizedTest = vectorize(testRDD, aspenModel)
      
      val labelAndPreds = vectorizedTest.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      
      val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / vectorizedTest.count()
      
      val msg = "Test Error = " + testErr
      println(msg)
      aspenModel.asInstanceOf[DecisionTreePredictionModel].setError(msg)
      
    } else if( KMeansPredictionModel.spark_kmeans_prediction.equals(aspenModel.getType)) {
      
      val parsedData = vectorizeNoLabels(trainRDD)
      
      
      var clustersCount = 10
      var clustersCountSetting = aspenModel.getModelConfig.getAlgorithmConfig.get("clustersCount");
      if(clustersCountSetting != null && clustersCountSetting.isInstanceOf[Number]) {
        clustersCount = clustersCountSetting.asInstanceOf[Number].intValue()
      }
      
      
      // Cluster the data into two classes using KMeans
      val numIterations = 20
      val clusters = KMeans.train(parsedData, clustersCount, numIterations)
      
      
      aspenModel.asInstanceOf[KMeansPredictionModel].setModel(clusters)
      
      // Evaluate clustering by computing Within Set Sum of Squared Errors
      val WSSSE = clusters.computeCost(parsedData)
      val msg = "Within Set Sum of Squared Errors = " + WSSSE

      println(msg)
      aspenModel.asInstanceOf[KMeansPredictionModel].setError(msg)
      
      
    } else if( NaiveBayesPredictionModel.spark_naive_bayes_prediction.equals(aspenModel.getType)) {
      
      val vectorized = vectorize(trainRDD, aspenModel);
      
      val model = NaiveBayes.train(vectorized, lambda = 1.0)
      
      aspenModel.asInstanceOf[NaiveBayesPredictionModel].setModel(model)
      
      val vectorizedTest = vectorize(testRDD, aspenModel)
      
      val predictionAndLabel = vectorizedTest.map(p => (model.predict(p.features), p.label))
      val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / vectorizedTest.count()
      
      val msg = "Accuracy: " + accuracy;
      
      println(msg)
      
      aspenModel.asInstanceOf[NaiveBayesPredictionModel].setError(msg);
      
    } else if( RandomForestPredictionModel.spark_randomforest_prediction.equals(aspenModel.getType ) ) {
      
      val vectorized = vectorize(trainRDD, aspenModel);
      
    	val numClasses = categories.length
 			val categoricalFeaturesInfo = catMap.toMap
 			val numTrees = 20 // Use more in practice.
 			val featureSubsetStrategy = "auto" // Let the algorithm choose.
 			val impurity = "gini"
 			val maxDepth = 20
 			val maxBins = 32
      
    	val model = RandomForest.trainClassifier(vectorized, numClasses, categoricalFeaturesInfo,
    			numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    			
    			//not until spark 1.3.0
    			//model.save(sc, "myModelPath")
    			
      aspenModel.asInstanceOf[RandomForestPredictionModel].setModel(model)
      
    	// Evaluate model on training instances and compute training error
    	/*
     val labelAndPreds = vectorized.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
     }
      val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / vectorized.count
      println("Training Error = " + trainErr)
      println("Learned classification tree model:\n" + model)
    	 */
    	
    	println("Testing ...")
    	
    	println("Test documents count: " + testRDD.count())
    	
    	val vectorizedTest = vectorize(testRDD, aspenModel)
    	
    	
//    val predictionAndLabel = vectorizedTest.map(p => (model.predict(p.features), p.label))
//    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / vectorizedTest.count()
//
//    println("Accuracy: " + accuracy)
    	
    	// Evaluate model on test instances and compute test error
    	val labelAndPreds = vectorizedTest.map { point =>
    	  val prediction = model.predict(point.features)
    	  (point.label, prediction)
    	}
      
    	val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / vectorizedTest.count()
//    println("Learned classification forest model:\n" + model.toDebugString)
    			
      val msg = "Test Error = " + testErr
      println(msg)
      
      aspenModel.asInstanceOf[RandomForestPredictionModel].setError(msg)
      
    } 

    
    //model packaging is now implemented in the model itsefl
    
    aspenModel.persist(modelFS, outputContainerPath, zipContainer || jarContainer)
    
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
 
  def vectorizeNoLabels ( trainRDD: RDD[(String, Array[Byte])] ) : RDD[Vector] = {
    
    return null
    
  }
  
  def vectorizeNoLabelsX ( trainRDD: RDD[(String, String, String)], dictionary: HashMap[String, Int]) : RDD[Vector]  = {
    
     val vectorized = trainRDD.map { gidNewsgroupText =>

//      val catgoryID: Double = categories.indexOf(gidNewsgroupText._2);

      var index2Value: Map[Int, Double] = Map[Int, Double]()

    /*
    for( entity <- gidNewsgroupText._4 ) {
      val index = dictionary.getOrElse(ENTITY + entity, -1)
      if(index >= 0) {
        index2Value += (index -> 1d)
      }
    }
    
    for( spantype <- gidNewsgroupText._5 ) {
      val index = dictionary.getOrElse(SPANTYPE + spantype, -1)
      if(index >= 0) {
        index2Value += (index -> 1d)
      }
    }
    */
    
    
    /*
    var msg : Message = null
    
    val inputObjects = VitalSigns.get().decodeBlock(gidNewsgroupText._2, 0, gidNewsgroupText._2.length)
    
    for(g <- inputObjects) {
      if(g.isInstanceOf[Message]) {
        msg = g.asInstanceOf[Message]
      }
    }
    
    if(msg == null) throw new RuntimeException("No 20 news message found in block")
    
    val text = msg.getProperty("title").toString() + " " + msg.getProperty("body").toString()
    */
    
      val text = gidNewsgroupText._3
    
      val words = text.toLowerCase().split("\\s+")

      for (x <- words ) {

        val index = dictionary.getOrElse(x, -1)

        if (index >= 0) {

          var v = index2Value.getOrElse(index, 0D);
          v = v + 1
          index2Value += (index -> v)

        }

      }

      val s = index2Value.toSeq.sortWith({ (p1, p2) =>
        p1._1 < p2._1
      })

      Vectors.sparse(dictionary.size, s)

    }
    
    return vectorized
    
  }
  
  def vectorize (trainRDD: RDD[(String, Array[Byte])], model: AspenModel) : RDD[LabeledPoint] = {
    
    val vectorized = trainRDD.map { pair =>

      
      val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
            
      val vitalBlock = new VitalBlock(inputObjects)
      
      val ex = new FeatureExtraction(model.getModelConfig, model.getAggregationResults)
      
      val featuresMap = ex.extractFeatures(vitalBlock)
      
      
      
      val catgoryID: Double = categories.indexOf(gidNewsgroupText._2);

      var index2Value: Map[Int, Double] = Map[Int, Double]()

    /*
    for( entity <- gidNewsgroupText._4 ) {
      val index = dictionary.getOrElse(ENTITY + entity, -1)
      if(index >= 0) {
        index2Value += (index -> 1d)
      }
    }
    
    for( spantype <- gidNewsgroupText._5 ) {
      val index = dictionary.getOrElse(SPANTYPE + spantype, -1)
      if(index >= 0) {
        index2Value += (index -> 1d)
      }
    }
    */
    
    
    /*
    var msg : Message = null
    
    val inputObjects = VitalSigns.get().decodeBlock(gidNewsgroupText._2, 0, gidNewsgroupText._2.length)
    
    for(g <- inputObjects) {
      if(g.isInstanceOf[Message]) {
        msg = g.asInstanceOf[Message]
      }
    }
    
    if(msg == null) throw new RuntimeException("No 20 news message found in block")
    
    val text = msg.getProperty("title").toString() + " " + msg.getProperty("body").toString()
    */
    
      val text = gidNewsgroupText._3
    
      val words = text.toLowerCase().split("\\s+")

      for (x <- words ) {

        val index = dictionary.getOrElse(x, -1)

        if (index >= 0) {

          var v = index2Value.getOrElse(index, 0D);
          v = v + 1
          index2Value += (index -> v)

        }

      }

      val s = index2Value.toSeq.sortWith({ (p1, p2) =>
        p1._1 < p2._1
      })

      LabeledPoint(catgoryID, Vectors.sparse(dictionary.size, s))

    }
    
    return vectorized
    
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