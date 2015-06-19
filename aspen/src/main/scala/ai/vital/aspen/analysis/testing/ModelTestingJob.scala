package ai.vital.aspen.analysis.testing

import ai.vital.aspen.job.AbstractJob
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Text
import ai.vital.hadoop.writable.VitalBytesWritable
import spark.jobserver.SparkJobValidation
import spark.jobserver.SparkJobInvalid
import spark.jobserver.SparkJobValid
import ai.vital.aspen.groovy.modelmanager.ModelManager
import ai.vital.aspen.config.AspenConfig
import ai.vital.aspen.groovy.AspenGroovyConfig
import ai.vital.aspen.model.AspenDecisionTreePredictionModel
import ai.vital.aspen.model.AspenNaiveBayesPredictionModel
import ai.vital.aspen.model.AspenRandomForestPredictionModel
import ai.vital.aspen.model.AspenRandomForestRegressionModel
import ai.vital.aspen.groovy.modelmanager.AspenModel
import ai.vital.vitalsigns.VitalSigns
import scala.collection.JavaConversions._
import ai.vital.domain.TargetNode
import ai.vital.vitalsigns.model.property.IProperty
import java.util.ArrayList
import ai.vital.vitalservice.segment.VitalSegment
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.query.querybuilder.VitalBuilder
import ai.vital.vitalservice.query.VitalGraphQuery
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalsigns.model.GraphMatch
import ai.vital.vitalsigns.model.property.URIProperty
import ai.vital.vitalsigns.block.CompactStringSerializer
import ai.vital.aspen.model.AspenKMeansPredictionModel
import org.apache.spark.Accumulator
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.vitalservice.model.App
import ai.vital.aspen.model.AspenCollaborativeFilteringPredictionModel
import ai.vital.aspen.groovy.featureextraction.FeatureExtraction
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.Rating
import scala.collection.mutable.MutableList
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import ai.vital.aspen.model.AspenLinearRegressionModel
import ai.vital.vitalsigns.model.URIReference
import org.apache.spark.mllib.regression.LabeledPoint
import ai.vital.vitalsigns.model.VITAL_Category
import ai.vital.aspen.model.AspenSVMWithSGDPredictionModel
import ai.vital.predictmodel.CategoricalFeature
import ai.vital.aspen.model.AspenLogisticRegressionPredictionModel
import ai.vital.aspen.model.AspenDecisionTreeRegressionModel
import ai.vital.aspen.model.AspenGradientBoostedTreesPredictionModel
import ai.vital.aspen.model.AspenGradientBoostedTreesRegressionModel
import ai.vital.aspen.model.AspenIsotonicRegressionModel
import ai.vital.aspen.model.AspenGaussianMixturePredictionModel
import ai.vital.aspen.model.PredictionModel
import ai.vital.aspen.analysis.training.ModelTrainingJob

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
    )
  }
  
  def runJob(sc: SparkContext, jobConfig: Config): Any = {

    val modelPath = new Path(jobConfig.getString(modelOption.getLongOpt))

    val inputName = jobConfig.getString(inputOption.getLongOpt)
    
    val serviceProfile = getOptionalString(jobConfig, profileOption)
        
    var inputRDDName : String = null
    if(inputName.startsWith("name:")) {
      inputRDDName = inputName.substring("name:".length())
    }
    
    val hadoopConfig = new Configuration()
    
    println("Input name/path: " + inputName)
    println("Model path: " + modelPath)
    
    println("service profile: " + serviceProfile)
    
//    val modelManager = new ModelManager()
    val mt2c = AspenGroovyConfig.get.modelType2Class
    mt2c.putAll(getModelManagerMap())
    
    val modelManager = new ModelManager()
    println("Loading model ...")
    val aspenModel = modelManager.loadModel(modelPath.toUri().toString())
    
    println("Model loaded successfully")

    
    if(serviceProfile != null) {
        VitalServiceFactory.setServiceProfile(serviceProfile)
    }
    
    VitalSigns.get.setVitalService(VitalServiceFactory.getVitalService)
    
    
      //URI, category, text
    var inputBlockRDD : RDD[(String, Array[Byte])] = null
    
    
    if(inputRDDName == null) {
      
      println("loading data from path...")
      
        val inputPath = new Path(inputName)
        
        val inputFS = FileSystem.get(inputPath.toUri(), hadoopConfig)
        
        if (!inputFS.exists(inputPath) /*|| !inputFS.isDirectory(inputPath)*/) {
          throw new RuntimeException("Input test path does not exist " + /*or is not a directory*/ ": " + inputPath.toString())
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
              aspenModel.getFeatureExtraction.composeBlock(vitalBlock)
              
              (pair._1.toString(), VitalSigns.get.encodeBlock(vitalBlock.toList()))
              
            } else {
              
              (pair._1.toString(), pair._2.get)
            }
          }
          
        }
        
    } else {
      
      println("loading data from named rdd...")
      
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
          aspenModel.getFeatureExtraction.composeBlock(vitalBlock)
              
          (pair._1.toString(), VitalSigns.get.encodeBlock(vitalBlock.toList()))
              
        } else {
              
          pair
              
        }
        
      }
      
    }
    
    if(aspenModel.isInstanceOf[AspenCollaborativeFilteringPredictionModel]) {
      
      //collaborative filtering model requires active spark context, it means it cannot be used inside workers
      //only one active spark context may be 
      
      val cfpm = aspenModel.asInstanceOf[AspenCollaborativeFilteringPredictionModel]
      
      cfpm.sc = sc
      
      cfpm.initModel()
      
      val acc = sc.accumulator(0);
      val samplesCount = sc.accumulator(0);
      
      val ratingsAll = inputBlockRDD.map { pair =>
        
        val block : java.util.List[GraphObject] = VitalSigns.get.decodeBlock(pair._2, 0, pair._2.length)
        
        val vitalBlock = new VitalBlock(block)
        
 //in collaborative filtering there are no target nodes in input data, use feature extractor and get the special feature, then compare with target score values
        val fe = aspenModel.getFeatureExtraction
        
        val featuresMap = fe.extractFeatures(vitalBlock)
        
        val ratingVal = featuresMap.get(AspenCollaborativeFilteringPredictionModel.feature_rating)
        
        val userURI = featuresMap.get(AspenCollaborativeFilteringPredictionModel.feature_user_uri)
        
        val productURI = featuresMap.get(AspenCollaborativeFilteringPredictionModel.feature_product_uri)
        
        var rating : Rating = null;
        
        if(ratingVal == null || userURI == null || productURI == null) {
          
        } else {
          
        	val userID = cfpm.getUserURI2ID().get(userURI.asInstanceOf[String])
          val productID = cfpm.getProductURI2ID().get(productURI.asInstanceOf[String])
          var ratingDouble = ratingVal.asInstanceOf[Number].doubleValue()
          
          if(userID != null && productID != null) {
            
        	  rating = new Rating(userID, productID, ratingDouble)
            
        	  samplesCount.add(1)
            
          }

          
          
          
        }
        
        acc.add(1)
        
        (rating)
        
      }
      
      
      val ratings = ratingsAll.filter { r => r != null } 
      ratings.cache();
      
      val usersProducts = ratings.map( r => {
        (r.user, r.product)
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
          
      val msg = "All samples: " + acc.value + "\nValid samples count: " + samplesCount + "\nMean Squared Error = " + MSE
          
      println(msg)
        
      return msg
      
    }
    
    if(aspenModel.isInstanceOf[AspenLinearRegressionModel]
    || aspenModel.isInstanceOf[AspenRandomForestRegressionModel]
    || aspenModel.isInstanceOf[AspenDecisionTreeRegressionModel]
    || aspenModel.isInstanceOf[AspenGradientBoostedTreesRegressionModel]
    || aspenModel.isInstanceOf[AspenIsotonicRegressionModel]) {
      
      var slrm : AspenLinearRegressionModel = null 
      
      var rfrm : AspenRandomForestRegressionModel = null
      
      var dtrm : AspenDecisionTreeRegressionModel = null
      
      var gbtrm : AspenGradientBoostedTreesRegressionModel = null
      
      var sirm : AspenIsotonicRegressionModel = null
      
      if( aspenModel.isInstanceOf[AspenLinearRegressionModel] ) {
        
    	  slrm = aspenModel.asInstanceOf[AspenLinearRegressionModel]
        
      } else if(aspenModel.isInstanceOf[AspenDecisionTreeRegressionModel]) {
        
        dtrm = aspenModel.asInstanceOf[AspenDecisionTreeRegressionModel]
        
      } else if(aspenModel.isInstanceOf[AspenGradientBoostedTreesRegressionModel]) {
        
        gbtrm = aspenModel.asInstanceOf[AspenGradientBoostedTreesRegressionModel]
        
      } else if(aspenModel.isInstanceOf[AspenIsotonicRegressionModel]) {
      
        sirm = aspenModel.asInstanceOf[AspenIsotonicRegressionModel]
      
      } else {
        
        rfrm = aspenModel.asInstanceOf[AspenRandomForestRegressionModel]
        
      }
      
      
      
      val valuesAndPreds = inputBlockRDD.map { pair =>
        
        
        val block : java.util.List[GraphObject] = VitalSigns.get.decodeBlock(pair._2, 0, pair._2.length)
        
        val vitalBlock = new VitalBlock(block)
      
        var point : LabeledPoint = null
        
        var prediction = 0D;
        
        if(slrm != null) {
          
        	val features = slrm.getFeatureExtraction.extractFeatures(vitalBlock)
          point = slrm.vectorizeLabels(vitalBlock, features)
          //also scale it 
          prediction = slrm.model.predict(slrm.scaleVector(point.features))
          
          prediction = slrm.scaledBack(prediction)
          
        } else if(dtrm != null) {

          val features = dtrm.getFeatureExtraction.extractFeatures(vitalBlock)
          point = dtrm.vectorizeLabels(vitalBlock, features)
          
          prediction = dtrm.model.predict(point.features)
          
        } else if(gbtrm != null) {
          
          val features = gbtrm.getFeatureExtraction.extractFeatures(vitalBlock)
          point = gbtrm.vectorizeLabels(vitalBlock, features)
          
          prediction = gbtrm.model.predict(point.features) 
          
        } else if(sirm != null) {
        
          val features = sirm.getFeatureExtraction.extractFeatures(vitalBlock)
          
          point = sirm.vectorizeLabels(vitalBlock, features)
          
          prediction = sirm.model.predict(point.features(0))
        
        } else {
          
        	val features = rfrm.getFeatureExtraction.extractFeatures(vitalBlock)
          point = rfrm.vectorizeLabels(vitalBlock, features)
        	prediction = rfrm.model.predict(point.features)
          
          
        }
        
        (point.label, prediction)
        
      }
      
      val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
      
      val msg = "training Mean Squared Error = " + MSE
        
      println(msg)
        
      return msg
      
      
      
    }
    
    //clustering stats
    var clustersStats : java.util.List[Accumulator[Int]] = null
    
    var clustersCount : java.lang.Integer = null 
    
    if(aspenModel.isInstanceOf[AspenKMeansPredictionModel]) {
      clustersCount = aspenModel.asInstanceOf[AspenKMeansPredictionModel].getClustersCount()
    } else if(aspenModel.isInstanceOf[AspenGaussianMixturePredictionModel]) {
      val gmpm = aspenModel.asInstanceOf[AspenGaussianMixturePredictionModel]
      gmpm.sc = sc
      clustersCount = gmpm.k
      
    }
    
    if(clustersCount != null) {
      clustersStats = new ArrayList[Accumulator[Int]]()
      var x = 0
      while( x < clustersCount) {
        clustersStats.add(sc.accumulator(0))
        x = x+1
      }
      
    }
    
    
    var reduced : (Int, Int, Int, Int) = null 
    
    if(aspenModel.isInstanceOf[AspenGaussianMixturePredictionModel]) {
      
//      throw new RuntimeException("DISABLED!")
      val gmpm = aspenModel.asInstanceOf[AspenGaussianMixturePredictionModel]
      
      val vectorized = ModelTrainingJob.vectorizeNoLabels(inputBlockRDD, gmpm)
      
      //XXX temporarily use the only option available
      val clusters = gmpm.model.predict(vectorized)

      var total = clusters.map { x => 

        clustersStats.get( x ).add(1)
        
      }.count().toInt
      
      reduced = (total, 0, 0, 0)
      
    } else { 
    
    
    //matched, morethan1 input target, no targets
    val results : RDD[(Int, Int, Int, Int)] = inputBlockRDD.map { pair =>
      
      if(VitalSigns.get.getCurrentApp == null) {
        val app = new App()
        app.setID("app")
        VitalSigns.get.setCurrentApp(app);
      }
      
      val block : java.util.List[GraphObject] = VitalSigns.get.decodeBlock(pair._2, 0, pair._2.length)
        
      val vitalBlock = new VitalBlock(block)
      
      var targetValue : String = null 
      
      var matched = 0
      
      var moreThanOne = 0
      
      var noTargets = 0
      
//      for( b <- block ) {
//        if(b.isInstanceOf[TargetNode]) {
//          val pv = b.getProperty("targetStringValue");
//          if(pv != null) {
//            if(targetValue != null) {
//              moreThanOne = 1           
//            }
//            targetValue = pv.asInstanceOf[IProperty].toString()
//          }
//        }
//      }
      
      
      if(aspenModel.asInstanceOf[PredictionModel].isClustering()) {
        
    	  val predictions = aspenModel.predict(vitalBlock)
        
        for(p <- predictions) {
                
          if(p.isInstanceOf[TargetNode]) {
                  
            val pv = p.getProperty("targetDoubleValue");
          
            if(pv != null) {
            
              val prediction = pv.asInstanceOf[IProperty].rawValue()
              
              clustersStats.get( prediction.asInstanceOf[Double].intValue() ).add(1)
            }
          }
        }
        
      } else {
        
        //use train block to extract data
          
        val ex = aspenModel.getFeatureExtraction
      
        val featuresMap = ex.extractFeatures(vitalBlock)
        
        var category : VITAL_Category = null
        
        var booleanCategory : java.lang.Boolean = null
        
        try {
        	val categoryx = aspenModel.getModelConfig.getTrainFeature.getFunction.call(vitalBlock, featuresMap)
          if(categoryx != null) {
            if(categoryx.isInstanceOf[VITAL_Category]) {
            	category = categoryx.asInstanceOf[VITAL_Category]
            } else if(categoryx.isInstanceOf[java.lang.Boolean]) {
              booleanCategory = categoryx.asInstanceOf[java.lang.Boolean]
            }            
          }
        } catch { case ex : Exception =>{
          ex.printStackTrace()
        }
        } 

        if(category != null || booleanCategory != null) {
          
           val predictions = aspenModel.predict(vitalBlock)
              
              for(p <- predictions) {
                
                if(p.isInstanceOf[TargetNode]) {
                  
                  if(aspenModel.getTrainFeatureType.equals(classOf[CategoricalFeature])) {
                    
                	  val pv = p.getProperty("targetStringValue");
                	  if(pv != null) {              
                		  val prediction = pv.asInstanceOf[IProperty].toString()
                				  
                				  if(category.getURI.equals(prediction)) {
                					  matched = 1
                				  }
                		  
                	  }
                    
                  //regression
                  } else {
                    
                    val pv = p.getProperty("targetDoubleValue");
                    if(pv != null) {
                      
                    	val prediction = pv.asInstanceOf[IProperty].rawValue().asInstanceOf[Double]
                      
                      if(booleanCategory.booleanValue() == (prediction.doubleValue() == 1) ) {
                        matched = 1
                      }
                      
                    }
                    
                  }
                  
                  
                }
                
              }
          
        } else {
          
          noTargets = 1 
          
        }
          
        
//    	  if(targetValue != null) {
//    		  
//    		  val predictions = aspenModel.predict(vitalBlock)
//    				  
//    				  for(p <- predictions) {
//    					  
//    					  if(p.isInstanceOf[TargetNode]) {
//    						  
//    						  val pv = p.getProperty("targetStringValue");
//    						  if(pv != null) {              
//    							  val prediction = pv.asInstanceOf[IProperty].toString()
//    									  
//    									  if(targetValue.equals(prediction)) {
//    										  matched = 1
//    									  }
//    							  
//    						  }
//    						  
//    					  }
//    					  
//    				  }
//    		  
//    	  } else {
//    		  noTargets = 1
//    	  }
        
      }
       
      

      (1, matched, moreThanOne, noTargets)
      
    }
    
    reduced = results.reduce { ( a, b ) =>
      (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4)
    }
    }
    
    val samples = reduced._1 - reduced._4
    
    val output = new StringBuilder("Total test samples: " + reduced._1);
    
    if(clustersStats != null) {
      
      output.append("\nClusters count: " + clustersStats.size())
      
      var x = 0
      for(cs <- clustersStats) {
        
        output.append("\nCluster #" + x + ": " + cs.value)
        
        x=x+1
      }
      
    } else {
      
    	output.append("\nSamples with targets: " + ( samples) )
    	output.append("\nSamples with more than 1 input target: " + reduced._3)
    	output.append("\nCorrect predictions: " + reduced._2)
    	
    	if(samples > 0) {
    		
    		output.append("\nAccuracy: " + (reduced._2.doubleValue() / samples.doubleValue()))
    		
    	} else {
    		
    		output.append("\nno accuracy stats - no samples with targets")
    		
    	}
      
    }
    
    
    println(output.toString())
    
    return output.toString()
    
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
