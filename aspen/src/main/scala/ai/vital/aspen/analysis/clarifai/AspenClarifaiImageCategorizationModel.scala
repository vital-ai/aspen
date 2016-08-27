package ai.vital.aspen.analysis.clarifai

import ai.vital.aspen.model.PredictionModel
import org.apache.commons.httpclient.HttpClient
import org.apache.spark.mllib.linalg.Vector
import java.io.InputStream
import java.io.File
import ai.vital.predictmodel.CategoricalFeature
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.predictmodel.Prediction
import com.vitalai.domain.nlp.Image
import java.util.Collection
import ai.vital.predictmodel.ImageFeature
import java.util.Arrays
import java.util.List
import java.util.Map
import ai.vital.predictmodel.Feature
import scala.collection.JavaConversions._
import java.lang.Long
import org.apache.commons.httpclient.methods.PostMethod
import org.codehaus.jackson.map.ObjectMapper
import java.util.LinkedHashMap
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity
import org.apache.commons.httpclient.methods.multipart.FilePart
import org.apache.commons.httpclient.methods.multipart.ByteArrayPartSource
import org.apache.commons.codec.binary.Base64
import org.apache.commons.httpclient.methods.multipart.Part
import org.apache.commons.httpclient.HttpMethodBase
import ai.vital.aspen.model.CategoryPrediction
import ai.vital.aspen.model.CategoriesListPrediction
import ai.vital.predictmodel.NumericalFeature
import org.apache.commons.httpclient.methods.GetMethod
import java.net.URLEncoder

object AspenClarifaiImageCategorizationModel {

  val jsonMapper = new ObjectMapper()
  
  val clarifai_image_categorization = "clarifai-image-categorization";
      
}

@SerialVersionUID(1L)
class AspenClarifaiImageCategorizationModel extends PredictionModel() {
  
  var clientID : String = null
  
  var clientSecret : String = null
  
  var accessToken : String = null
  
  var tokenExpDate : Long = null
  
  var tokenTimestamp : Long = null
  
  @transient
  var client : HttpClient = null 
  
	def deserializeModel(stream: InputStream): Unit = {
    throw new RuntimeException("No model binary expected")
	}
  
	def supportedType(): String = {
			AspenClarifaiImageCategorizationModel.clarifai_image_categorization
	}
  
	def persistFiles(tempDir: File): Unit = {

    //DO NOTHING
    
	}

  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = classOf[NumericalFeature]
  
  def doPredict(v: Vector): Double = {
    throw new RuntimeException("This method shouldn't be used") 
  }

  def isTestedWithTrainData(): Boolean = {
	  false
	}

  def onAlgorithmConfigParam(key: String, value: java.io.Serializable): Boolean = {

     // Build the recommendation model using ALS
    if("clientID".equals(key)) {
      
      if(!value.isInstanceOf[String]) ex(key + " must be a string")
      clientID=value.asInstanceOf[String]
      
    } else if("clientSecret".equals(key)) {
        
      	if(!value.isInstanceOf[String]) ex(key + " must be a string")
        clientSecret=value.asInstanceOf[String]
        
    } else {
      
      return false
      
    }
    
    return true
    
	}
  
  private def _refreshToken() : Unit = {
    
    if(accessToken == null || System.currentTimeMillis() >= tokenExpDate.longValue()) {
      
      accessToken = null
      
      if(client == null) client = new HttpClient()
      
      var authPost = new PostMethod("https://api.clarifai.com/v1/token/")
      authPost.addParameter("client_id", clientID);
      authPost.addParameter("client_secret", clientSecret);
      authPost.addParameter("grant_type", "client_credentials")
      
      
      var respStatus : Integer = null
      var respString : String = ""
      
      try {
      
        respStatus = client.executeMethod(authPost)
        
        try {
          respString = authPost.getResponseBodyAsString()
        } catch {
          case ex : Exception => {
            
          }
        }
      } finally {
        authPost.releaseConnection()
      }
      
      if(respStatus.intValue() != 200) {
        throw new RuntimeException("Clarifai auth exception, HTTP status: " + respStatus + ", response: " + respString)
      }
      
      val authRes = AspenClarifaiImageCategorizationModel.jsonMapper.readValue(respString, classOf[LinkedHashMap[String, Object]])
      
      accessToken = authRes.get("access_token").asInstanceOf[String]
      
      val expiresIn = authRes.get("expires_in").asInstanceOf[Number].longValue()
      
      //subtract 60 seconds 
      tokenExpDate = System.currentTimeMillis() + ( expiresIn * 1000L ) - 60000L
      
      
    }
    
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
    
    var imageData = img.getProperty("imageData")
    if(imageData == null) throw new RuntimeException("No imageData property")
    
    var imageDataString : String = imageData.toString()
    
    
    var fileName = img.getProperty("name")
    if(fileName != null) {
      fileName = fileName.toString()
    } else {
      fileName = "unknown_file"
    }
    
    var imageURL : String = null
    
    if(imageDataString.startsWith("http:") || imageDataString.startsWith("https:") ) {
      imageURL = imageDataString  
    } else {
      
      //extract raw base64 string
      if(imageDataString.startsWith("data:")) {
        
        val firstComma = imageDataString.indexOf(',')
        if(firstComma < 0) throw new RuntimeException("data: uri scheme must have a comma in it to separate data")
//        data:image/png;base64,
        
        imageDataString = imageDataString.substring(firstComma + 1)
        
      }
      
    }
    
    _refreshToken()
    
    var respString : String = null
    
    var method : HttpMethodBase = null
    
    if(imageURL != null) {
     
      val gm = new GetMethod("https://api.clarifai.com/v1/tag?url=" + URLEncoder.encode(imageURL, "UTF-8"))
      
      gm.addRequestHeader("Authorization", "Bearer " + accessToken);

      method = gm
      
    } else {
     
      val pm = new PostMethod("https://api.clarifai.com/v1/tag");
      
      pm.addRequestHeader("Authorization", "Bearer " + accessToken);
        
      val bytes = Base64.decodeBase64(imageDataString)
        
      val parts = Array(new FilePart("encoded_data", new ByteArrayPartSource("file", bytes)).asInstanceOf[Part]);
        
      val mpre = new MultipartRequestEntity(parts, pm.getParams)
      
      pm.setRequestEntity(mpre)
     
      method = pm
      
    }

    if(client == null) client = new HttpClient()

    try {
      
      val httpStatus = client.executeMethod(method)

      try {
    	  respString = method.getResponseBodyAsString()
      } catch {
        case e : Exception => {
          
        }
      }
      
       if(httpStatus.intValue() != 200) {
        throw new RuntimeException("Clarifai auth exception, HTTP status: " + httpStatus + ", response: " + respString)
      }
      
    } finally {
      method.releaseConnection()
    }
    
    
    val tagRes = AspenClarifaiImageCategorizationModel.jsonMapper.readValue(respString, classOf[LinkedHashMap[String, Object]])
    
    val statusCode = tagRes.get("status_code").asInstanceOf[String]

    if(!statusCode.equalsIgnoreCase("ok")) {
    	val statusMsg = tagRes.get("status_msg").asInstanceOf[String] 
      throw new RuntimeException("Clarifai API error, code: " + statusCode + ", message: " + statusMsg)
    }
    
    val results = tagRes.get("results").asInstanceOf[List[Map[String, Object]]]
    
    val outPrediction = new CategoriesListPrediction()
    
    for( result <- results ) {
      
      val resultObj = result.asInstanceOf[Map[String, Object]]
      
      val resultElement = resultObj.get("result").asInstanceOf[Map[String,Object]]
     
      val tagElement = resultElement.get("tag").asInstanceOf[Map[String,Object]]
      
      val conceptIds = tagElement.get("concept_ids").asInstanceOf[List[String]]
      
      val classes = tagElement.get("classes").asInstanceOf[List[String]]
      
 		  val probs = tagElement.get("probs").asInstanceOf[List[Number]]

      var i = 0
      
      while ( i < conceptIds.length ) {
        
        val conceptId = conceptIds.get(i).asInstanceOf[String]
        
        val class_ = classes.get(i)
        
        val prob = probs.get(i).doubleValue()
        
        i = i+1
        
        val p = new CategoryPrediction()
        
        p.categoryURI = conceptId
        
        p.categoryLabel = class_
        
        p.score = prob

        outPrediction.predictions.add(p)
        
      }
      
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