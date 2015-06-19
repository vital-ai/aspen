package ai.vital.aspen.model

import org.apache.spark.mllib.tree.model.RandomForestModel
import java.io.InputStream
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.mllib.linalg.Vector
import org.apache.commons.io.IOUtils
import java.io.File
import java.io.FileOutputStream
import org.apache.commons.io.FileUtils
import java.nio.charset.StandardCharsets
import ai.vital.predictmodel.CategoricalFeature

object AspenRandomForestPredictionModel {
  
	val spark_randomforest_prediction = "spark-randomforest-prediction";

}

@SerialVersionUID(1L)
class AspenRandomForestPredictionModel extends AspenRandomForestBaseModel {
  
  def supportedType(): String = {
    AspenRandomForestPredictionModel.spark_randomforest_prediction
  }

  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = {
    classOf[CategoricalFeature]
  }
  
  def getDefaultImpurity(): String = {
    "gini"
  }
}