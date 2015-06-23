package ai.vital.aspen.analysis.testing.impl

import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.groovy.predict.tasks.TestModelIndependentTask
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.groovy.predict.tasks.LoadModelTask
import ai.vital.aspen.groovy.modelmanager.AspenModel
import ai.vital.aspen.model.AspenCollaborativeFilteringPredictionModel
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.VitalSigns
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import org.apache.spark.mllib.recommendation.Rating
import ai.vital.aspen.model.AspenLinearRegressionModel
import ai.vital.aspen.model.AspenRandomForestRegressionModel
import ai.vital.aspen.model.AspenDecisionTreeRegressionModel
import ai.vital.aspen.model.AspenGradientBoostedTreesRegressionModel
import ai.vital.aspen.model.AspenIsotonicRegressionModel
import ai.vital.aspen.model.AspenLinearRegressionModel
import ai.vital.aspen.model.AspenRandomForestRegressionModel
import ai.vital.aspen.model.AspenDecisionTreeRegressionModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.Accumulator
import ai.vital.aspen.model.AspenKMeansPredictionModel
import ai.vital.aspen.model.AspenGaussianMixturePredictionModel
import java.util.ArrayList
import ai.vital.aspen.analysis.training.ModelTrainingJob
import org.apache.spark.rdd.RDD
import ai.vital.aspen.model.PredictionModel
import ai.vital.vitalsigns.model.property.IProperty
import ai.vital.domain.TargetNode
import ai.vital.vitalsigns.model.VITAL_Category
import ai.vital.predictmodel.CategoricalFeature
import scala.collection.JavaConversions._
import ai.vital.vitalservice.model.App
import ai.vital.aspen.groovy.predict.tasks.TestModelTask

class TestModelIndependentTaskImpl(job: AbstractJob, task: TestModelIndependentTask) extends TaskImpl[TestModelIndependentTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {

  }

  def execute(): Unit = {

    val s = impl()
    
    task.getParamsMap.put(TestModelTask.STATS_STRING, s)
    
  }
  
  def impl() : String = {
    
    var modelO = task.getParamsMap.get(LoadModelTask.LOADED_MODEL_PREFIX + task.modelPath)
    if(modelO == null) throw new RuntimeException("Loaded model not found, path: " + task.modelPath)
    
    if(!modelO.isInstanceOf[AspenModel]) throw new RuntimeException("Invalid object type: " + modelO.getClass.getCanonicalName)

    val aspenModel = modelO.asInstanceOf[AspenModel]
    
    val sc = job.sparkContext
    
    val inputBlockRDD = job.getDataset(task.datasetName)
    
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
    
    return output.toString()
    
  }
  
}