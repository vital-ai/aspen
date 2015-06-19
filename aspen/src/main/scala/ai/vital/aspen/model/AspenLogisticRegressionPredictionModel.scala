package ai.vital.aspen.model

import java.io.InputStream
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.commons.lang3.SerializationUtils
import org.apache.commons.io.IOUtils
import ai.vital.predictmodel.CategoricalFeature
import java.io.File
import java.io.FileOutputStream
import org.apache.commons.io.FileUtils
import java.nio.charset.StandardCharsets

object AspenLogisticRegressionPredictionModel {
  val spark_logistic_regression_prediction = "spark-logistic-regression-prediction"  
}

@SerialVersionUID(1L)
class AspenLogisticRegressionPredictionModel extends PredictionModel {
  
  var model : LogisticRegressionModel = null
  
  def deserializeModel(stream: InputStream): Unit = {
    
    val deserializedModel : LogisticRegressionModel= SerializationUtils.deserialize(IOUtils.toByteArray(stream))
    
    model = deserializedModel match {
      case x: LogisticRegressionModel => x
      case _ => throw new ClassCastException
    }
      
  }

  def doPredict(v: Vector): Double = {
    model.predict(v)
  }

  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = {
    classOf[CategoricalFeature]
  }

  def isTestedWithTrainData(): Boolean = {
    false
  }

  def onAlgorithmConfigParam(param: String, value: java.io.Serializable): Boolean = {
    return false
  }

  def persistFiles(tempDir: File): Unit = {
    val os = new FileOutputStream(new File(tempDir, model_bin))
    SerializationUtils.serialize(model, os)
    os.close()
    
    if(error != null) {
      FileUtils.writeStringToFile(new File(tempDir, error_txt), error, StandardCharsets.UTF_8.name())
    }
  }

  def supportedType(): String = {
    AspenLogisticRegressionPredictionModel.spark_logistic_regression_prediction
  }
}