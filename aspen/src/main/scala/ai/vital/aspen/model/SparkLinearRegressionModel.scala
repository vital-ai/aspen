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
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.SparkContext

object SparkLinearRegressionModel {
  
  val spark_linear_regression = "spark-linear-regression";
}

class SparkLinearRegressionModel extends PredictionModel {

  var model : LinearRegressionModel = null
  
  def setModel(_model: LinearRegressionModel) : Unit = {
    model = _model
  }
  
  def getModel() : LinearRegressionModel = {
    model
  }
  
  def deserializeModel(stream: InputStream): Unit = {

      val deserializedModel : LinearRegressionModel = SerializationUtils.deserialize(IOUtils.toByteArray(stream))
    
      model = deserializedModel match {
        case x: LinearRegressionModel => x
        case _ => throw new ClassCastException
      }
  }

  def doPredict(v: Vector): Int = {
     throw new RuntimeException("shouldn't be called")
  }
  
  @Override
  override def _predict(vitalBlock : VitalBlock, featuresMap : java.util.Map[String, Object]) : Prediction = {
    
    val objects : java.util.List[GraphObject] = null
    
    val value = model.predict(vectorizeNoLabels(vitalBlock, featuresMap))
    
    val pred = new RegressionPrediction
    pred.value = value
    
    return pred
    
  }  

  def isCategorical(): Boolean = {
    false
  }

  @Override
  def isTestedWithTrainData() : Boolean = {
    return true;
  }

  def supportedType(): String = {
    SparkLinearRegressionModel.spark_linear_regression
  }

  def persistFiles(tempDir: File): Unit = {
    
    val os = new FileOutputStream(new File(tempDir, model_bin))
    SerializationUtils.serialize(model, os)
    os.close()
    
    if(error != null) {
      FileUtils.writeStringToFile(new File(tempDir, error_txt), error, StandardCharsets.UTF_8.name())
    }
    
  }
  
}