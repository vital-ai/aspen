package ai.vital.aspen.model

import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import java.io.InputStream
import org.apache.commons.lang3.SerializationUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.mllib.linalg.Vector
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.predictmodel.Prediction
import ai.vital.vitalsigns.model.GraphObject
import java.io.File
import java.io.FileOutputStream
import org.apache.commons.io.FileUtils
import java.nio.charset.StandardCharsets
import ai.vital.aspen.groovy.featureextraction.Dictionary
import scala.collection.JavaConversions._
import ai.vital.domain.TargetNode
import ai.vital.vitalsigns.model.property.IProperty
import java.util.ArrayList
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object CollaborativeFilteringPredictionModel {

  val spark_collaborative_filtering_prediction = "spark-collaborative-filtering-prediction";
 
  //return string
	val feature_user_uri = "feature_user_uri"
  
  //return string
	val feature_product_uri = "feature_product_uri"
  
	val feature_rating = "feature_rating"
  
  
}

@SerialVersionUID(1L)
class CollaborativeFilteringPredictionModel extends PredictionModel {

  //this is not serialized
  @transient
  var model : MatrixFactorizationModel = null
  
  @transient
  var sc : SparkContext = null  
  
  var wrappedModel : MatrixFactorizationModelWrapper = null;
  
  val useruri2id_tsv = "useruri_2_id.tsv"
  
  val producturi2id_tsv = "producturi_2_id.tsv"
  
  var userURI2ID : Dictionary = null
  
  var productURI2ID : Dictionary = null
  
  
  def setModel(_model: MatrixFactorizationModel) : Unit = {
    model = _model
    wrappedModel = new MatrixFactorizationModelWrapper()
    wrappedModel.productFeatures = model.productFeatures.collect()
    wrappedModel.rank = model.rank
    wrappedModel.userFeatures = model.userFeatures.collect()
  }
  
  def getModel() : MatrixFactorizationModel = {
    model
  }
  
  def setUserURI2ID(_userURI2ID : Dictionary) : Unit = {
    userURI2ID = _userURI2ID
  }
  
  def getUserURI2ID() : Dictionary = {
    userURI2ID
  }
  
  def setProductURI2ID(_productURI2ID : Dictionary) : Unit = {
    productURI2ID = _productURI2ID
  }
  
  def getProductURI2ID() : Dictionary = {
    productURI2ID
  }
  
  def supportedType(): String = {
    return CollaborativeFilteringPredictionModel.spark_collaborative_filtering_prediction
  }

  def deserializeModel(stream: InputStream): Unit = {
    
    IOUtils.toByteArray(stream)
    
//      val deserializedModel : MatrixFactorizationModel = SerializationUtils.deserialize(IOUtils.toByteArray(stream))
//    
//      model = deserializedModel match {
//        case x: MatrixFactorizationModel => x
//        case _ => throw new ClassCastException
//      }
      
  }

  /**
   * In collaborative filtering this method is not used
   */
  def doPredict(v: Vector): Int = {
    throw new RuntimeException("Collaborative filtering does not use this method!")
  }
  
  
  /**
   * useful for tests
   */
  def getRating(userURI : String , productURI : String ) : Double = {
    
    initModel()
    
    val userID = userURI2ID.get(userURI)
    if(userID == null) throw new RuntimeException("userID not found for URI: " + userURI)
    
    val productID = productURI2ID.get(productURI)
    if(productID == null) throw new RuntimeException("productID not found for URI: " + productURI)
    
    return model.predict(userID, productID)
    
  }
  
  //it is important to initialize the model before it's being used
  def initModel() {
    
    if(model == null) {
      //init model from wrapped model
      if(wrappedModel == null) throw new RuntimeException("Cannot restore wrapped model")
      
      if(sc == null) sc = new SparkContext("local", "collaborative-filtering-model-" + modelConfig.getName, new SparkConf())
      
      model = new MatrixFactorizationModel(wrappedModel.rank, sc.parallelize(wrappedModel.userFeatures.toSeq), sc.parallelize(wrappedModel.productFeatures.toSeq))
      
    }
    
  }
  
  @Override
  override def _predict(vitalBlock : VitalBlock, featuresMap : java.util.Map[String, Object]) : Prediction = {
    
    initModel()
    
    //each model has to provide two numerical feature
    
    //max recommendataions
    
    var maxPredictions = 10
    
    for(x <- vitalBlock.toList() ) {
      if(x.isInstanceOf[TargetNode]) {
        maxPredictions = x.asInstanceOf[TargetNode].getProperty("targetScore").asInstanceOf[IProperty].rawValue().asInstanceOf[Number].intValue()
      }
    }
    
    val userURI = featuresMap.get(CollaborativeFilteringPredictionModel.feature_user_uri)
    
    if(userURI == null) throw new RuntimeException("No " + CollaborativeFilteringPredictionModel.feature_user_uri + " feature provided!");
    
    val userID = userURI2ID.get(userURI.asInstanceOf[String])
    
    if(userID == null) throw new RuntimeException("userID not found for URI: " + userURI)
    
    val ratings = model.recommendProducts(userID, maxPredictions)
    
    var uris = new ArrayList[String]()
    var scores = new ArrayList[java.lang.Double]()
    
    for(r <- ratings) {
      
      if(userID.intValue() != r.user.intValue()) throw new RuntimeException("Got someone elses recommendation: " + userID + " " + r.user)

      val uri = productURI2ID.getReverse(r.product)
      if(uri == null) throw new RuntimeException("Product URI not found: " + r.product)
      
      uris.add(uri)
      scores.add(r.rating)
      
    }
    
    val rp = new RecommendationPrediction()
    
    rp.productURIs = uris
    rp.productRatings = scores
    
//    val userURI = featuresMap.get("user-uri").asInstanceOf[String];
    return rp
    
    
//    val objects : java.util.List[GraphObject] = null
//    
//    val categoryID = doPredict(vectorizeNoLabels(vitalBlock, featuresMap))
//    
//    val category = trainedCategories.getCategories.get(categoryID.intValue())
//    
//    val pred = new CategoryPrediction
//    pred.category = category
//    pred.categoryID = categoryID
//    
//    return pred
    
  } 
  
  @Override
  def persistFiles(tempDir : File) : Unit = {

    if(wrappedModel != null) {
      val os = new FileOutputStream(new File(tempDir, model_bin))
      SerializationUtils.serialize(wrappedModel, os)
      os.close()
    }  
    
    if(error != null) {
      FileUtils.writeStringToFile(new File(tempDir, error_txt), error, StandardCharsets.UTF_8.name())
    }
    
    if(userURI2ID != null) {
      val outputStream = new FileOutputStream(new File(tempDir, useruri2id_tsv))
      userURI2ID.saveTSV(outputStream)
      outputStream.close()
    }
    
    if(productURI2ID != null) {
      val outputStream = new FileOutputStream(new File(tempDir, producturi2id_tsv))
      productURI2ID.saveTSV(outputStream)
      outputStream.close()
    }
    
  }
  
  @Override
  override def close() : Unit = {
    super.close()
    
    if(sc != null) {
      sc.stop()
    }
  }
  
  @Override
  def isTestedWithTrainData() : Boolean = {
    return true;
  }
  
  @Override
  def isCategorical() : Boolean = {
      return false;
  }
  
}