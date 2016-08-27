package ai.vital.aspen.model

import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.linalg.Vector
import java.io.InputStream
import org.apache.commons.lang3.SerializationUtils
import org.apache.commons.io.IOUtils
import java.io.File
import java.io.FileOutputStream
import org.apache.commons.io.FileUtils
import java.nio.charset.StandardCharsets
import ai.vital.predictmodel.BinaryFeature

object AspenSVMWithSGDPredictionModel {

  val spark_svm_w_sgd_prediction = "spark-svm-w-sgd-prediction";
  
}

@SerialVersionUID(1L)
class AspenSVMWithSGDPredictionModel extends PredictionModel {

  var model : SVMModel = null

  var numIterations = 100
  
  def deserializeModel(stream: InputStream): Unit = {
    
      val deserializedModel : SVMModel = PredictionModelUtils.deserialize(IOUtils.toByteArray(stream))//SerializationUtils.deserialize(IOUtils.toByteArray(stream))
    
      model = deserializedModel match {
        case x: SVMModel => x
        case _ => throw new ClassCastException
      }
      
  }
  
  def doPredict(v: Vector): Double = {
    model.predict(v)
  }
  
  def isTestedWithTrainData(): Boolean = {
    false
  }
  
  def onAlgorithmConfigParam(param: String, value: java.io.Serializable): Boolean = {
    
    if("numIterations".equals(param)){
      
      if(!value.isInstanceOf[Number]) ex(param + " must be an int/long number")
      
      numIterations = value.asInstanceOf[Number].intValue()
      
      if(numIterations < 1) ex(param + " must be >= 1") 
      
    } else {
      
      return false
      
    }
    
    return true
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
    AspenSVMWithSGDPredictionModel.spark_svm_w_sgd_prediction
  }

  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = {
    classOf[BinaryFeature]
  }
  
  
}