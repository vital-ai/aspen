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
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.predictmodel.Prediction
import java.io.Serializable

object KMeansPredictionModel {
  
	val spark_kmeans_prediction = "spark-kmeans-prediction";

}

@SerialVersionUID(1L)
class KMeansPredictionModel extends PredictionModel {

  var clustersCount = 10
  
  var numIterations = 20

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
    throw new RuntimeException("shouldn't be called!")
    //return model.predict(v).intValue()
  }
  
  @Override
  override def _predict(vitalBlock : VitalBlock, featuresMap : java.util.Map[String, Object]) : Prediction = {
    
    val clusterID = model.predict(vectorizeNoLabels(vitalBlock, featuresMap)).intValue()
    
    var cp = new ClusterPrediction()
    cp.clusterID = clusterID
    return cp
    
    
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
  
  @Override
  def onAlgorithmConfigParam(param: String, value: Serializable): Boolean = {
    
     if("clustersCount".equals(param)) {
      
      if(!value.isInstanceOf[Number]) ex(param + " must be an int/long number")
      
      clustersCount = value.asInstanceOf[Number].intValue()
      
      if(clustersCount < 2) ex(param + " must be >= 2")
      
    } else if("numIterations".equals(param)){
      
      if(!value.isInstanceOf[Number]) ex(param + " must be an int/long number")
      
      numIterations = value.asInstanceOf[Number].intValue()
      
      if(numIterations < 1) ex(param + " must be >= 1") 
      
    } else {
      
      return false
      
    }
    
    return true
  }
}