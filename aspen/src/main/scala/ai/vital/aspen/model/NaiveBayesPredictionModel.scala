package ai.vital.aspen.model

import org.apache.spark.mllib.classification.NaiveBayesModel
import java.io.InputStream
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.mllib.linalg.Vector
import org.apache.commons.io.IOUtils
import java.io.File
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets
import org.apache.commons.io.FileUtils
import java.io.Serializable

object NaiveBayesPredictionModel {
  
  val spark_naive_bayes_prediction = "spark-naive-bayes-prediction";
  
}

@SerialVersionUID(1L)
class NaiveBayesPredictionModel extends PredictionModel {

  var model : NaiveBayesModel = null;
  
  
  //algorithm
  var lambda = 1.0d
  
  def supportedType(): String = {
    return NaiveBayesPredictionModel.spark_naive_bayes_prediction
  }
  
  def setModel(_model: NaiveBayesModel) : Unit = {
    model = _model
  }
  
  def getModel() : NaiveBayesModel = {
    model
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
  
  @Override
  def persistFiles(tempDir : File) : Unit = {

    val os = new FileOutputStream(new File(tempDir, model_bin))
    SerializationUtils.serialize(model, os)
    os.close()
    
    if(error != null) {
      FileUtils.writeStringToFile(new File(tempDir, error_txt), error, StandardCharsets.UTF_8.name())
    }
  }
  
  @Override
  def isTestedWithTrainData() : Boolean = {
    return false;
  }
  
  @Override
  def isCategorical() : Boolean = {
      return true;
  }

  def onAlgorithmConfigParam(key: String, value: Serializable): Boolean = {
    
    if("lamda".equals(key)) {
      
      if(!value.isInstanceOf[Number]) ex(key + " must be a number")
      
      lambda = value.asInstanceOf[Number].doubleValue()
      
      if(lambda <= 0d) ex(key + " must be  > 0.0")
      
    } else {
      return false
    }

    return true
    
  }
  
}