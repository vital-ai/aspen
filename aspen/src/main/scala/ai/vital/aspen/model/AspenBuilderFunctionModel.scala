package ai.vital.aspen.model

import java.io.File
import java.io.InputStream
import java.util.Arrays
import java.util.Collection
import java.util.Map
import org.apache.spark.mllib.linalg.Vector
import ai.vital.predictmodel.BinaryFeature
import ai.vital.predictmodel.Prediction
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.predictmodel.Feature
import ai.vital.predictmodel.StringFeature
import ai.vital.predictmodel.DateFeature
import ai.vital.predictmodel.CategoricalFeature
import ai.vital.predictmodel.DateTimeFeature
import ai.vital.predictmodel.GeoLocationFeature
import ai.vital.predictmodel.ImageFeature
import ai.vital.predictmodel.NumericalFeature
import ai.vital.predictmodel.OrdinalFeature
import ai.vital.predictmodel.TextFeature
import ai.vital.predictmodel.URIFeature
import ai.vital.predictmodel.WordFeature

object AspenBuilderFunctionModel {
  
    val builder_function = "builder-function";

}

@SerialVersionUID(1L)
/**
 * Special model type to allow custom builder function logic.
 * The target function simply receives the BuilderFunctionPrediction instance with model config in it
 */
class AspenBuilderFunctionModel extends PredictionModel {
  
  val config = new java.util.HashMap[String, Object]()
  
  @Override
  override def mustUsePredictClosure() : Boolean = {
    return true
  }
  
	def deserializeModel(stream: InputStream): Unit = {
    throw new RuntimeException("No model binary expected")
	}
  
	def supportedType(): String = {
			AspenBuilderFunctionModel.builder_function
	}
  
	def persistFiles(tempDir: File): Unit = {

    //DO NOTHING
    
	}

  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = classOf[BinaryFeature]
  
  def doPredict(v: Vector): Double = {
    throw new RuntimeException("This method shouldn't be used") 
  }

  def isTestedWithTrainData(): Boolean = {
	  false
	}

  def onAlgorithmConfigParam(key: String, value: java.io.Serializable): Boolean = {

    //accept everything
    
    config.put(key, value.asInstanceOf[Object])
    
    return true
    
	}
  
  @Override
  override def _predict(vitalBlock : VitalBlock, featuresMap : Map[String, Object]): Prediction = {
    
    throw new RuntimeException("Should use closure instead")
//    val fmp = new BuilderFunctionPrediction()
//    
//    fmp.config = config
//    
//    return fmp
     
  }
  
  @Override
  override def getSupportedFeatures() : Collection[Class[_ <: Feature]] = {
    return Arrays.asList( 
        classOf[BinaryFeature],
        classOf[CategoricalFeature],
        classOf[DateFeature],
        classOf[DateTimeFeature],
        classOf[GeoLocationFeature],
        classOf[ImageFeature],
        classOf[NumericalFeature],
        classOf[OrdinalFeature],
        classOf[StringFeature],
        classOf[TextFeature],
        classOf[URIFeature],
        classOf[WordFeature]
    ) 
  }
 
  
  @Override
  override def onResourcesProcessed(): Unit = {
    
  }
  

}