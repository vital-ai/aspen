package ai.vital.aspen.model

import org.apache.spark.mllib.classification.NaiveBayesModel
import java.io.InputStream
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.mllib.linalg.Vector
import org.apache.commons.io.IOUtils

object NaiveBayesPredictionModel {
  
  val spark_naive_bayes_prediction = "spark-naive-bayes-prediction";
  
}

@SerialVersionUID(1L)
class NaiveBayesPredictionModel extends PredictionModel {

  var model : NaiveBayesModel = null;
  
  def supportedType(): String = {
    return NaiveBayesPredictionModel.spark_naive_bayes_prediction
  }

  def deserializeModel(stream: InputStream): Unit = {
    
      val deserializedModel : NaiveBayesModel = SerializationUtils.deserialize(IOUtils.toByteArray(stream))
    
      model = deserializedModel match {
        case x: NaiveBayesModel => x
        case _ => throw new ClassCastException
      }
      
  }

  def doPredict(v: Vector): Int = {
    return model.predict(v).intValue()
  }
  
}