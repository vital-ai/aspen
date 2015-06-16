package ai.vital.aspen.model

import org.apache.spark.mllib.clustering.PowerIterationClusteringModel
import org.apache.spark.mllib.linalg.Vector
import java.io.InputStream
import org.apache.commons.lang3.SerializationUtils
import org.apache.commons.io.IOUtils
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.predictmodel.Prediction
import java.io.File
import java.io.FileOutputStream
import org.apache.commons.io.FileUtils
import ai.vital.predictmodel.NumericalFeature
import java.nio.charset.StandardCharsets

object SparkPowerIterationClusteringModel {
  
  val spark_power_ieration_clustering = "spark-power-ieration-clustering";

}


@SerialVersionUID(1L)
class SparkPowerIterationClusteringModel extends PredictionModel {

  var k = 3
  
  var maxIterations = 20

  var model : PowerIterationClusteringModel = null;

  def setModel(_model: PowerIterationClusteringModel) : Unit = {
    model = _model
  }
  
  def supportedType(): String = {
    return SparkPowerIterationClusteringModel.spark_power_ieration_clustering
  }

  def deserializeModel(stream: InputStream): Unit = {
    
      val deserializedModel : PowerIterationClusteringModel = SerializationUtils.deserialize(IOUtils.toByteArray(stream))
    
      model = deserializedModel match {
        case x: PowerIterationClusteringModel => x
        case _ => throw new ClassCastException
      }
      
  }

  def doPredict(v: Vector): Double = {
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
  def onAlgorithmConfigParam(param: String, value: Serializable): Boolean = {
    
     if("k".equals(param)) {
      
      if(!value.isInstanceOf[Number]) ex(param + " must be an int/long number")
      
      k = value.asInstanceOf[Number].intValue()
      
      if(k < 2) ex(param + " must be >= 2")
      
    } else if("maxIterations".equals(param)){
      
      if(!value.isInstanceOf[Number]) ex(param + " must be an int/long number")
      
      maxIterations = value.asInstanceOf[Number].intValue()
      
      if(maxIterations < 1) ex(param + " must be >= 1") 
      
    } else {
      
      return false
      
    }
    
    return true
  }

  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = {
    classOf[NumericalFeature]
  }
  
}