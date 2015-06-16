package ai.vital.aspen.model

import java.io.InputStream
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.mllib.linalg.Vector
import org.apache.commons.io.IOUtils
import java.io.File
import java.io.FileOutputStream
import org.apache.commons.io.FileUtils
import java.nio.charset.StandardCharsets
import ai.vital.predictmodel.CategoricalFeature
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

@SerialVersionUID(1L)
abstract class GradientBoostedTreesBaseModel extends PredictionModel {
  
  var model : GradientBoostedTreesModel = null;
  
  //algorithm settings
  var numIterations = 3 // Use more in practice.
  var maxDepth = 20
  
  def deserializeModel(stream: InputStream): Unit = {
    
      val deserializedModel : GradientBoostedTreesModel = SerializationUtils.deserialize(IOUtils.toByteArray(stream))
    
      model = deserializedModel match {
        case x: GradientBoostedTreesModel => x
        case _ => throw new ClassCastException
      }
      
  }

  def doPredict(v: Vector): Double = {
    return model.predict(v)
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
  
  def onAlgorithmConfigParam(key: String, value: java.io.Serializable): Boolean = {

    if("maxDepth".equals(key)) {
      
      if(!value.isInstanceOf[Number]) ex(key + " must be an int/long number")
      
      maxDepth = value.asInstanceOf[Number].intValue()
      
      if(maxDepth < 1) ex(key + " must be >= 1")
      
    } else if("numIterations".equals(key)) {
      
      if(!value.isInstanceOf[Number]) ex(key + " must be an int/long number")
      
      numIterations = value.asInstanceOf[Number].intValue()
      
      if(numIterations < 1) ex(key + " must be >= 1")
      
    } else {
      return false
    }
    
    return true
    
  }

}