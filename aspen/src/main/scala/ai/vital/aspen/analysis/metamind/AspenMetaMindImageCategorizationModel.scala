package ai.vital.aspen.analysis.metamind

import java.io.File
import java.io.InputStream
import java.util.Arrays
import java.util.Collection
import java.util.HashMap
import java.util.List
import java.util.Map
import scala.collection.JavaConversions._
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.PostMethod
import org.apache.spark.mllib.linalg.Vector
import ai.vital.aspen.model.CategoriesListPrediction
import ai.vital.aspen.model.CategoryPrediction
import ai.vital.aspen.model.PredictionModel
import ai.vital.predictmodel.CategoricalFeature
import ai.vital.predictmodel.Prediction
import ai.vital.predictmodel.StringFeature
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.vitalsigns.model.VITAL_Category
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import ai.vital.predictmodel.Feature
import ai.vital.predictmodel.ImageFeature
import com.vitalai.domain.nlp.Image

object AspenMetaMindImageCategorizationModel {
  
    val metamind_image_categorization = "metamind-image-categorization";


}

@SerialVersionUID(1L)
class AspenMetaMindImageCategorizationModel extends PredictionModel() {
  
  var apiKey : String = null
  
  var classifierID : String = null
  
  @transient
  var client : HttpClient = null 
  
	def deserializeModel(stream: InputStream): Unit = {
    throw new RuntimeException("No model binary expected")
	}
  
	def supportedType(): String = {
			AspenMetaMindImageCategorizationModel.metamind_image_categorization
	}
  
	def persistFiles(tempDir: File): Unit = {

    //DO NOTHING
    
	}

  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = classOf[CategoricalFeature]
  
  def doPredict(v: Vector): Double = {
    throw new RuntimeException("This method shouldn't be used") 
  }

  def isTestedWithTrainData(): Boolean = {
	  false
	}

  def onAlgorithmConfigParam(key: String, value: java.io.Serializable): Boolean = {

     // Build the recommendation model using ALS
    if("apiKey".equals(key)) {
      
      if(!value.isInstanceOf[String]) ex(key + " must be a string")
      apiKey=value.asInstanceOf[String]
      
    } else if("classifierID".equals(key)) {
        
      	if(!value.isInstanceOf[String]) ex(key + " must be a string")
        classifierID=value.asInstanceOf[String]
        
      } else {
      
      return false
      
    }
    
    return true
    
	}
  
  @Override
  override def _predict(vitalBlock : VitalBlock, featuresMap : Map[String, Object]): Prediction = {
    
    var img : Image = null;
    
    for(f <- featuresMap.values()) {
      
      if(f.isInstanceOf[Image]) {
        img = f.asInstanceOf[Image]
      }
      
    }
    
    if(img == null) throw new RuntimeException("No image feature provided")
    
    var imageURL = img.getProperty("imageData")
    if(imageURL == null) throw new RuntimeException("No image-content-url feature")
    
    imageURL = imageURL.toString()
    
    val pm = new PostMethod("https://www.metamind.io/vision/classify");
        
    pm.addRequestHeader("Authorization", "Basic " + apiKey);
       
    val m = new HashMap[String, Object]();
    m.put("classifier_id", "imagenet-1k-net");
    m.put("image_url", imageURL.asInstanceOf[String]);
    
    val json = JsonOutput.toJson(m);
    pm.setRequestBody(json);
    
    if(client == null) client = new HttpClient()
    
    val status = client.executeMethod(pm);
        
    var resp = "";
    try {
      resp = pm.getResponseBodyAsString();
    } catch {
      case e: Exception => {
      }
    }
        
    pm.releaseConnection();
    
    if(status < 200 || status > 299) {
      throw new Exception(s"MetaMind API returned status ${status} - ${resp}")
    }
    
    val response = new JsonSlurper().parseText(resp).asInstanceOf[Map[String, Object]]
    
    val predictions = response.get("predictions").asInstanceOf[List[Map[String, Object]]]
    
    val outPrediction = new CategoriesListPrediction()
    
    for(pred <- predictions) {
          
    	var vc : VITAL_Category = null
      
      val classID = pred.get("class_id").asInstanceOf[Number].intValue()
      
      val prob = pred.get("prob").asInstanceOf[Number].doubleValue()
      
      val cname = pred.get("class_name").asInstanceOf[String]
      
      val p = new CategoryPrediction()
      
      p.categoryID = classID
      p.score = prob 
      
      outPrediction.predictions.add(p)
      
    }
    
    return outPrediction
     
  }
  
  @Override
  override def getSupportedFeatures() : Collection[Class[_ <: Feature]] = {
    return Arrays.asList(
        classOf[ImageFeature]
    )
  }
 
  
  @Override
  override def onResourcesProcessed(): Unit = {
    
//    if(!modelBinaryLoaded) throw new Exception("Model was not loaded, make sure " + model_bin + " file is in the model files")))))))))
    
  }
  

}