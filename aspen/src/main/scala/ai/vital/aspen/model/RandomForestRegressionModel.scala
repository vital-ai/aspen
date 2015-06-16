package ai.vital.aspen.model

import org.apache.spark.mllib.linalg.Vector
import java.io.InputStream
import org.apache.commons.lang3.SerializationUtils
import org.apache.commons.io.IOUtils
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.predictmodel.Prediction
import ai.vital.vitalsigns.model.GraphObject
import java.io.File
import java.io.FileOutputStream
import org.apache.commons.io.FileUtils
import java.nio.charset.StandardCharsets
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.model.RandomForestModel
import ai.vital.predictmodel.NumericalFeature

object RandomForestRegressionModel {
  
  val spark_randomforest_regression = "spark-randomforest-regression";

}

@SerialVersionUID(1L)
class RandomForestRegressionModel extends RandomForestBaseModel {

  def supportedType(): String = {
    RandomForestRegressionModel.spark_randomforest_regression
  }

  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = {
    classOf[NumericalFeature]
  }

  def getDefaultImpurity(): String = {
    "variance"
  }
  
}