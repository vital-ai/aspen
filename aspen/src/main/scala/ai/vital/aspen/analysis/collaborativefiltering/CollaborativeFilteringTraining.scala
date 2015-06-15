package ai.vital.aspen.analysis.collaborativefiltering

import ai.vital.aspen.analysis.training.AbstractTraining
import ai.vital.aspen.model.CollaborativeFilteringPredictionModel
import ai.vital.aspen.util.SetOnceHashMap
import java.io.Serializable
import org.apache.spark.rdd.RDD
import ai.vital.vitalsigns.VitalSigns
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import java.util.HashMap
import ai.vital.aspen.groovy.featureextraction.Dictionary
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS


object CollaborativeFilteringTraining {
  
  def collaborativeFilteringCollectData(globalContext: SetOnceHashMap, trainRDD : RDD[(String, Array[Byte])], model: CollaborativeFilteringPredictionModel) : RDD[(String, String, Double)] = {
    
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
class CollaborativeFilteringTraining(model: CollaborativeFilteringPredictionModel) extends AbstractTraining[CollaborativeFilteringPredictionModel](model) {

    
  def train(globalContext: SetOnceHashMap, trainRDD: RDD[(String, Array[Byte])]): Serializable = {

     //first pass to collect user and products ids -> uris
    val values = CollaborativeFilteringTraining.collaborativeFilteringCollectData(globalContext, trainRDD, model)
        
    val ratings = values.map(quad => {
      new Rating(model.getUserURI2ID().get(quad._1), model.getProductURI2ID().get(quad._2), quad._3)
    })
        
    ratings.cache()
        
    globalContext.put("collaborative-filtering-ratings", ratings)

    val rank = model.rank
    val lambda = model.lambda
    val iterations = model.iterations
    
    val trained = ALS.train(ratings, rank, iterations, lambda)
        
    model.setModel(trained)
    
    
    val usersProducts = values.map( triple => {
      (model.getUserURI2ID().get(triple._1).toInt, model.getProductURI2ID().get(triple._2).toInt )
    }) 
          
    val predictions = model.getModel().predict(usersProducts).map { case Rating(user, product, rate) => 
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
    model.setError(msg)
        
    return trained

  }
}