package ai.vital.aspen.model

import java.io.InputStream
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.clustering.KMeansModel

object KMeansPredictionModel {
  
	val spark_kmeans_prediction = "spark-kmeans-prediction";
  
}

class KMeansPredictionModel extends PredictionModel {


  var model : KMeansModel = null;
  
  def supportedType(): String = {
    return KMeansPredictionModel.spark_kmeans_prediction
  }

  def deserializeModel(stream: InputStream): Unit = {
    
      val deserializedModel : KMeansModel = SerializationUtils.deserialize(stream)
    
      model = deserializedModel match {
        case x: KMeansModel => x
        case _ => throw new ClassCastException
      }
      
  }

  def doPredict(v: Vector): Int = {
    return model.predict(v).intValue()
  }
  
}