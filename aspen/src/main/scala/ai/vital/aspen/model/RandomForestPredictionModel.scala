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

object RandomForestPredictionModel {
  
	val spark_randomforest_prediction = "spark-randomforest-prediction";
  
}

@SerialVersionUID(1L)
class RandomForestPredictionModel extends PredictionModel {
  
  var model : RandomForestModel = null;
  
  def setModel(_model: RandomForestModel) : Unit = {
    model = _model
  }
  
  def getModel() : RandomForestModel = {
    model
  }
  
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
  def isSupervised() : Boolean = {
    return true;
  }
  
  @Override
  def isCategorical() : Boolean = {
      return true;
  }
  
}