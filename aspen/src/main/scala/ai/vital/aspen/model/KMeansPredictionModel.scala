package ai.vital.aspen.model

import java.io.InputStream
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.commons.io.IOUtils
import java.io.File
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets
import org.apache.commons.io.FileUtils

object KMeansPredictionModel {
  
	val spark_kmeans_prediction = "spark-kmeans-prediction";
  
}

class KMeansPredictionModel extends PredictionModel {


  var model : KMeansModel = null;

  def setModel(_model: KMeansModel) : Unit = {
    model = _model
  }
  
  def supportedType(): String = {
    return KMeansPredictionModel.spark_kmeans_prediction
  }

  def deserializeModel(stream: InputStream): Unit = {
    
      val deserializedModel : KMeansModel = SerializationUtils.deserialize(IOUtils.toByteArray(stream))
    
      model = deserializedModel match {
        case x: KMeansModel => x
        case _ => throw new ClassCastException
      }
      
  }

  def doPredict(v: Vector): Int = {
    return model.predict(v).intValue()
  }
  
  override def isClustering() : Boolean = {
    true
  }
 
  def getClustersCount() : Int = {
    model.k
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
    return true;
  }
  
  @Override
  def isCategorical() : Boolean = {
      return false;
  }
}