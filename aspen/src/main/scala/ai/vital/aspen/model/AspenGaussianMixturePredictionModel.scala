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
import ai.vital.predictmodel.NumericalFeature
import org.apache.spark.mllib.clustering.GaussianMixtureModel
import org.apache.spark.SparkContext
import scala.collection.immutable.Seq
import breeze.stats.distributions.MultivariateGaussian
import breeze.linalg.{DenseVector => BreezeVector}
import org.apache.spark.mllib.util.MLUtils

object AspenGaussianMixturePredictionModel {
  
	val spark_gaussian_mixture_prediction = "spark-gaussian-mixture-prediction";

}

@SerialVersionUID(1L)
class AspenGaussianMixturePredictionModel extends PredictionModel {

  var k = 10
  
  @transient
  var sc : SparkContext = null  
  
  var model : GaussianMixtureModel = null;

  def supportedType(): String = {
    return AspenGaussianMixturePredictionModel.spark_gaussian_mixture_prediction
  }

  def deserializeModel(stream: InputStream): Unit = {
    
      val deserializedModel : GaussianMixtureModel = PredictionModelUtils.deserialize(IOUtils.toByteArray(stream))//SerializationUtils.deserialize(IOUtils.toByteArray(stream))
    
      model = deserializedModel match {
        case x: GaussianMixtureModel => x
        case _ => throw new ClassCastException
      }
      
  }

  def doPredict(v: Vector): Double = {
    throw new RuntimeException("shouldn't be called!")
    //return model.predict(v).intValue()
  }
  
  @Override
  override def _predict(vitalBlock : VitalBlock, featuresMap : java.util.Map[String, Object]) : Prediction = {

//    val softPredict = computeSoftAssignments(x.toBreeze.toDenseVector, model.gaussians, model.weights, k)
    
    val x = vectorizeNoLabels(vitalBlock, featuresMap)
    
//    val clusterID = model.predict(x)
    
    
    //XXX replace it with a non-spark context variant if available, 1.4.1
    val input = sc.parallelize(Seq(x))
    val clusterID = model.predict(input).collect()(0).intValue()
    
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
      
    } else {
      
      return false
      
    }
    
    return true
  }

  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = {
    classOf[NumericalFeature]
  }
 
}