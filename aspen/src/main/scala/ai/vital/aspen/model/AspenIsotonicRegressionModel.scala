package ai.vital.aspen.model

import java.io.InputStream
import org.apache.spark.mllib.regression.IsotonicRegressionModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.commons.lang3.SerializationUtils
import org.apache.commons.io.IOUtils
import ai.vital.predictmodel.NumericalFeature
import java.io.File
import java.io.FileOutputStream
import org.apache.commons.io.FileUtils
import java.nio.charset.StandardCharsets
import scala.collection.JavaConversions._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object AspenIsotonicRegressionModel {
  
  val spark_isotonic_regression = "spark-isotonic-regression";
  
}
@SerialVersionUID(1L)
class AspenIsotonicRegressionModel extends PredictionModel {
  
  var isotonic : java.lang.Boolean = true
  
  var model : IsotonicRegressionModel = null
  
  def deserializeModel(stream: InputStream): Unit = {
    
    val deserializedModel : IsotonicRegressionModel = PredictionModelUtils.deserialize(IOUtils.toByteArray(stream))//SerializationUtils.deserialize(IOUtils.toByteArray(stream))
    
    model = deserializedModel match {
      case x: IsotonicRegressionModel => x
      case _ => throw new ClassCastException
    }
  }

  def doPredict(v: Vector): Double = {
    
    if(v.size != 1) throw new RuntimeException("Expected exactly 1 numerical feature value!") 
    
    model.predict( v(0) )
    
  }

  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = {
    classOf[NumericalFeature]
  }

  def isTestedWithTrainData(): Boolean = {
    false
  }

  def onAlgorithmConfigParam(param: String, value: java.io.Serializable): Boolean = {

    if("isotonic".equals(param)) {
      
      if(!value.isInstanceOf[java.lang.Boolean]) ex(param  + " must be a boolean value")
      
      isotonic = value.asInstanceOf[java.lang.Boolean].booleanValue()
      
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

  override def validateConfig() : Unit = {
    
    super.validateConfig()
    
    var numericFeatures = 0
    var otherFeatures = 0
    
    for(x <- modelConfig.getFeatures ) {

      if(x.isInstanceOf[NumericalFeature]) {
        numericFeatures = numericFeatures + 1
      } else {
        otherFeatures = otherFeatures + 1
      }
      
    }
    
    if(numericFeatures != 1 || otherFeatures != 0) throw new RuntimeException("Isotonic regression model expects exactly 1 numerical feature, got " + numericFeatures + " numerical and " + otherFeatures + " other feature(s)")
    
  }
  
  def supportedType(): String = {
    AspenIsotonicRegressionModel.spark_isotonic_regression
  }

  def toTuple(vectorized: RDD[LabeledPoint]) : RDD[(Double, Double, Double)] = {
    
		  val training = vectorized.map { lp =>
        (lp.label, lp.features(0), 1.0)
      }
      
      training
      
  }
    

  
}