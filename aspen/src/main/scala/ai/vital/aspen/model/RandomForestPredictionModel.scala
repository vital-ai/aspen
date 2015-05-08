package ai.vital.aspen.model

import org.apache.spark.mllib.tree.model.RandomForestModel
import java.io.InputStream
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.mllib.linalg.Vector
import org.apache.commons.io.IOUtils

object RandomForestPredictionModel {
  
	val spark_randomforest_prediction = "spark-randomforest-prediction";
  
}

@SerialVersionUID(1L)
class RandomForestPredictionModel extends PredictionModel {
  

  var model : RandomForestModel = null;
  
  def supportedType(): String = {
    return RandomForestPredictionModel.spark_randomforest_prediction
  }

  def deserializeModel(stream: InputStream): Unit = {
    
      val deserializedModel : RandomForestModel = SerializationUtils.deserialize(IOUtils.toByteArray(stream))
    
      model = deserializedModel match {
        case x: RandomForestModel => x
        case _ => throw new ClassCastException
      }
      
  }

  def doPredict(v: Vector): Int = {
    return model.predict(v).intValue()
  }
 
  
  
}