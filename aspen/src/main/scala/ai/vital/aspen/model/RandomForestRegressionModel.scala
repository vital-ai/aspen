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

object RandomForestRegressionModel {
  
  val spark_randomforest_regression = "spark-randomforest-regression";

}

@SerialVersionUID(1L)
class RandomForestRegressionModel extends PredictionModel {

  var model : RandomForestModel = null
  
  
  //algorithm settings
  var numTrees = 3 // Use more in practice.
  var featureSubsetStrategy = "auto" // Let the algorithm choose.
  var impurity = "variance"
  var maxDepth = 4
  var maxBins = 32
  
  def setModel(_model: RandomForestModel) : Unit = {
    model = _model
  }
  
  def getModel() : RandomForestModel = {
    model
  }
  
  def deserializeModel(stream: InputStream): Unit = {

      val deserializedModel : RandomForestModel = SerializationUtils.deserialize(IOUtils.toByteArray(stream))
    
      model = deserializedModel match {
        case x: RandomForestModel => x
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
    RandomForestRegressionModel.spark_randomforest_regression
  }

  def persistFiles(tempDir: File): Unit = {
    
    val os = new FileOutputStream(new File(tempDir, model_bin))
    SerializationUtils.serialize(model, os)
    os.close()
    
    if(error != null) {
      FileUtils.writeStringToFile(new File(tempDir, error_txt), error, StandardCharsets.UTF_8.name())
    }
    
  }
  
  def onAlgorithmConfigParam(key: String, value: java.io.Serializable): Boolean = {

    if("featureSubsetStrategy".equals(key)) {
      
      featureSubsetStrategy = value.asInstanceOf[String]
      
    } else if("impurity".equals(key)) {
      
      impurity = value.asInstanceOf[String]
      
    } else if("maxDepth".equals(key)) {
      
      if(!value.isInstanceOf[Number]) ex(key + " must be an int/long number")
      
      maxDepth = value.asInstanceOf[Number].intValue()
      
      if(maxDepth < 1) ex(key + " must be >= 1")
      
    } else if("maxBins".equals(key)) {
      
      if(!value.isInstanceOf[Number]) ex(key + " must be an int/long number")
      
      maxBins = value.asInstanceOf[Number].intValue()
      
      if(maxBins < 1) ex(key + " must be >= 1")
      
    } else if("numTrees".equals(key)) {

      if(!value.isInstanceOf[Number]) ex(key + " must be an int/long number")
      
      numTrees = value.asInstanceOf[Number].intValue()
      
      if(numTrees < 1) ex(key + " must be >= 1")
      
    } else {
      return false
    }
    
    return true
    
  }
  
}