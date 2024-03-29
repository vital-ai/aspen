package ai.vital.aspen.analysis.alchemyapi

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
import ai.vital.predictmodel.TextFeature
import java.net.URL
import ai.vital.predictmodel.URIFeature
import ai.vital.vitalsigns.model.property.URIProperty

object AspenAlchemyAPICategorizationModel {
  
    val alchemy_api_categorization = "alchemy-api-categorization";


}

@SerialVersionUID(1L)
class AspenAlchemyAPICategorizationModel extends PredictionModel {
  
  var apiKey : String = null
  
  @transient
  var client : HttpClient = null 
  
	def deserializeModel(stream: InputStream): Unit = {
    throw new RuntimeException("No model binary expected")
	}
  
	def supportedType(): String = {
			AspenAlchemyAPICategorizationModel.alchemy_api_categorization
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
      
    } else {
      
      return false
      
    }
    
    return true
    
	}
  
  @Override
  override def _predict(vitalBlock : VitalBlock, featuresMap : Map[String, Object]): Prediction = {
    
    val content = new StringBuilder()
    
    var url : String = null
    
    for(f <- modelConfig.getFeatures ) {
      
      if(f.isInstanceOf[TextFeature]) {
        
        var c = featuresMap.get(f.getName)
        if(c != null) {
        	if(content.length > 0) { content.append(" ") }
        	content.append(c.asInstanceOf[String])
        }
        
        
      } else if(f.isInstanceOf[URIFeature]) {
        
        var c = featuresMap.get(f.getName)
        
        if(c != null) {
          if(c.isInstanceOf[String]) {
        	  url = c.asInstanceOf[String] 
          } else if(c.isInstanceOf[URIProperty]) {
            url = c.asInstanceOf[URIProperty].get
          }
        }
        
        
      }
      
    }
    
    var cs = content.toString()
    
    //check if url,
    
    var urlCase = false
    
    var endpoint : String = null
    
    if(cs.length() > 0) {
      
    	endpoint = "http://access.alchemyapi.com/calls/text/TextGetRankedTaxonomy"
        
    } else if(url != null) {
    	
    	urlCase = true
      
      endpoint = "http://access.alchemyapi.com/calls/url/URLGetRankedTaxonomy"
      
    } else {
      throw new RuntimeException("No text nor uri features")
    }
    
    
    
    val pm = new PostMethod(endpoint);

    
    //normal post
    pm.addParameter("apikey", apiKey)
    if(urlCase) {
    	pm.addParameter("url", url)
    } else {
    	pm.addParameter("text", cs)
    }
    pm.addParameter("outputMode", "json")
    
    if(client == null) client = new HttpClient()
    
    val statusCode = client.executeMethod(pm);
        
    var resp = "";
    try {
      resp = pm.getResponseBodyAsString();
    } catch {
      case e: Exception => {
      }
    }
        
    pm.releaseConnection();
    
    if(statusCode < 200 || statusCode > 299) {
      throw new Exception(s"AlchemyAPI returned status ${statusCode} - ${resp}")
    }
    
    
    /*
    {
    "status": "REQUEST_STATUS",
    "url": "REQUESTED_URL",
    "language": "DOCUMENT_LANGUAGE",
    "text": "DOCUMENT_TEXT"
    "taxonomy": [
        {
            "label": "DETECTED_CATEGORY"
            "score": "DETECTED_SCORE"
            "confident": "CONFIDENCE_FLAG"
        }
    ]
    }
     */
    
    
    val response = new JsonSlurper().parseText(resp).asInstanceOf[Map[String, Object]]
    
    //check status
    val status = response.get("status").asInstanceOf[String]
    
    if(!"OK".equalsIgnoreCase(status)) {
      throw new RuntimeException("AlchemyAPI status: " + status + " - " + response.get("statusInfo"))
    }
    
    val taxonomy = response.get("taxonomy").asInstanceOf[List[Map[String, Object]]]
    
    val outPrediction = new CategoriesListPrediction()
    
    for(pred <- taxonomy) {
          
      val label = pred.get("label").asInstanceOf[String]
      
      val score = java.lang.Double.parseDouble(pred.get("score").asInstanceOf[String])
      
//      val confident = pred.get("confident")
      
      
      
      val p = new CategoryPrediction()
      
      p.categoryLabel = label
      p.score = score
      
      outPrediction.predictions.add(p)
      
    }
    
    return outPrediction
     
  }
  
  @Override
  override def getSupportedFeatures() : Collection[Class[_ <: Feature]] = {
    return Arrays.asList(
        classOf[TextFeature],
        classOf[URIFeature]
    )
  }
 
  
  @Override
  override def onResourcesProcessed(): Unit = {
    
//    if(!modelBinaryLoaded))))) throw new Exception("Model was not loaded, make sure " + model_bin + " file is in the model files"
    
  }
  

}