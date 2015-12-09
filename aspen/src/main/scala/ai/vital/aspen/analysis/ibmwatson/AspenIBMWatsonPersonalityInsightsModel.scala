package ai.vital.aspen.analysis.ibmwatson

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
import org.apache.commons.httpclient.auth.AuthScope
import org.apache.commons.httpclient.UsernamePasswordCredentials
import org.codehaus.jackson.map.ObjectMapper
import ai.vital.aspen.model.BuilderFunctionPrediction
import java.util.ArrayList
import com.vitalai.domain.ibmwatson.PersonalityInsight
import com.vitalai.domain.ibmwatson.Big5
import com.vitalai.domain.ibmwatson.Extraversion
import com.vitalai.domain.ibmwatson.Agreeableness
import com.vitalai.domain.ibmwatson.Conscientiousness
import com.vitalai.domain.ibmwatson.EmotionalRange
import com.vitalai.domain.ibmwatson.Openness
import com.vitalai.domain.ibmwatson.Openness
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.properties.Property_hasName
import com.vitalai.domain.ibmwatson.properties.Property_hasCategory
import org.apache.commons.io.FileUtils
import java.text.SimpleDateFormat
import org.apache.commons.httpclient.methods.StringRequestEntity
import com.vitalai.domain.ibmwatson.Values
import com.vitalai.domain.ibmwatson.Needs
import com.vitalai.domain.ibmwatson.properties.Property_hasOpennessValue
import com.vitalai.domain.ibmwatson.properties.Property_hasAgreeablenessValue
import com.vitalai.domain.ibmwatson.properties.Property_hasConscientiousnessValue
import com.vitalai.domain.ibmwatson.properties.Property_hasEmotionalRangeValue
import com.vitalai.domain.ibmwatson.properties.Property_hasExtraversionValue
import com.vitalai.domain.ibmwatson.properties.Property_hasValue
import ai.vital.predictmodel.BinaryFeature

object AspenIBMWatsonPersonalityInsightsModel {
  
    val ibm_watson_personality_insights = "ibm-watson-personality-insights";

    val jsonMapper = new ObjectMapper()
    
}

@SerialVersionUID(1L)
class AspenIBMWatsonPersonalityInsightsModel extends PredictionModel {

  var apiUsername : String = null
  var apiPassword : String = null
  
  @transient
  var client : HttpClient = null 
  
	def deserializeModel(stream: InputStream): Unit = {
    throw new RuntimeException("No model binary expected")
	}
  
	def supportedType(): String = {
			AspenIBMWatsonPersonalityInsightsModel.ibm_watson_personality_insights
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

     // Build the recommendation model using ALS
    if("apiUsername".equals(key)) {
      
      if(!value.isInstanceOf[String]) ex(key + " must be a string")
      apiUsername = value.asInstanceOf[String]
      
    } else if("apiPassword".equals(key)) {
      
      if(!value.isInstanceOf[String]) ex(key + " must be a string")
      apiPassword = value.asInstanceOf[String]
      
    } else {
      
      return false
      
    }
    
    return true
    
	}
  
  @Override
  override def _predict(vitalBlock : VitalBlock, featuresMap : Map[String, Object]): Prediction = {
    
    val content = new StringBuilder()
    
    var url : String = null

    val languageF = featuresMap.get("language")
    
    //default
    var language : String = "en"
    
    if(languageF != null) {
      language = languageF.asInstanceOf[String]
      if(language.equals("en") || language.startsWith("en-")) {
      } else if(language.equals("es") || language.startsWith("es-")){
      } else {
        throw new Exception("Unsupported document language: " + language)
      }
    }
    
    for(f <- modelConfig.getFeatures ) {
      
      if(f.isInstanceOf[TextFeature]) {
        
        var c = featuresMap.get(f.getName)
        if(c != null) {
        	if(content.length > 0) { content.append(" ") }
        	content.append(c.asInstanceOf[String])
        }
        
        
//      } else if(f.isInstanceOf[URIFeature]) {
//        
//        var c = featuresMap.get(f.getName)
//        
//        if(c != null) {
//          if(c.isInstanceOf[String]) {
//        	  url = c.asInstanceOf[String] 
//          } else if(c.isInstanceOf[URIProperty]) {
//            url = c.asInstanceOf[URIProperty].get
//          }
//        }
//        
//        
      }
      
    }
    
    var cs = content.toString()
    
    return processContent(cs, language)
    
  }
  
  def processContent(cs : String, language : String ) : Prediction = { 
    
    var endpoint : String = "https://gateway.watsonplatform.net/personality-insights/api/v2/profile"
    
    val pm = new PostMethod(endpoint + "?include_raw=true");

    
//    pm.addRequestHeader("Content-Type", "text/plain; charset=utf-8")
    pm.addRequestHeader("Accept", "application/json; charset=utf-8")
    pm.addRequestHeader("Content-Language", language)
    //normal post
//    pm.addParameter("include_raw", "true")
//    pm.addParameter("body", cs)
    
    pm.setRequestEntity(new StringRequestEntity(cs, "text/plain", "utf-8"))
    
    if(client == null) {
      client = new HttpClient()
      client.getParams().setAuthenticationPreemptive(true)
      var credentials = new UsernamePasswordCredentials(apiUsername, apiPassword)
      client.getState().setCredentials(new AuthScope(AuthScope.ANY), credentials)
    }
    
    val statusCode = client.executeMethod(pm);
        
    var resp = "";
    try {
      resp = pm.getResponseBodyAsString();
    } catch {
      case e: Exception => {
      }
    }
    
    val sdf = new SimpleDateFormat("yyyy-MM-dd_HHmmss");
    FileUtils.writeStringToFile(new File("" + sdf.format(new java.util.Date()) +  ".json"), resp);
    
    pm.releaseConnection();
    
    if(statusCode < 200 || statusCode > 299) {
      throw new Exception(s"IBM Watson returned status ${statusCode} - ${resp}")
    }
    
    processResponse(resp)
    
  }
  
  def processResponse(resp : String) : Prediction = {
    
    val watsonProfile = AspenIBMWatsonPersonalityInsightsModel.jsonMapper.readValue(resp, classOf[IBMWatsonProfile])
    
    /*
     {
  "id": "*UNKNOWN*",
  "source": "*UNKNOWN*",
  "word_count": 142,
  "word_count_message": "There were 142 words in the input. We need a minimum of 3,500, preferably 6,000 or more, to compute a reliable estimate",
  "processed_lang": "en",
  "tree": {
    "id": "r",
    "name": "root",
    "children": [{
      "id": "personality",
      "name": "Big 5 ",
      "children": [{
     */
    
    val prediction = new BuilderFunctionPrediction()
    
    val list : List[PersonalityInsight] = new ArrayList[PersonalityInsight]()
    
    processTree(watsonProfile.tree, null, list)
    
    var big5 : Big5 = null
    
    //post process list
    for(pi <- list) {
      
      var rawValue = pi.getRaw(classOf[Property_hasValue]) 
      
      if(pi.getClass.equals(classOf[Big5])) {
        big5 = pi.asInstanceOf[Big5]
      } else if(pi.isInstanceOf[Agreeableness]) {
        big5.set(classOf[Property_hasAgreeablenessValue], rawValue)
      } else if(pi.isInstanceOf[Conscientiousness]) {
        big5.set(classOf[Property_hasConscientiousnessValue], rawValue)
      } else if(pi.isInstanceOf[EmotionalRange]) {
        big5.set(classOf[Property_hasEmotionalRangeValue], rawValue)
      } else if(pi.isInstanceOf[Extraversion]) {
        big5.set(classOf[Property_hasExtraversionValue], rawValue)
      } else if(pi.isInstanceOf[Openness]) {
        big5.set(classOf[Property_hasOpennessValue], rawValue)
      } 
      
    }
    
    
    prediction.value = list
    
    /*
    for(pred <- taxonomy) {
          
      val label = pred.get("label").asInstanceOf[String]
      
      val score = java.lang.Double.parseDouble(pred.get("score").asInstanceOf[String])
      
//      val confident = pred.get("confident")
      
      
      
      val p = new CategoryPrediction()
      
      p.categoryLabel = label
      p.score = score
      
      outPrediction.predictions.add(p)
      
    }
    */
    
    return prediction
     
  }
  
  def processTree(el: IBMWatsonTrait, parentNode : PersonalityInsight, list : List[PersonalityInsight]) : Unit = {
    
    //pass through duplicated parent elements
    if(el.id.endsWith("_parent")) {

      for(child <- el.children) {
        
    	  processTree(child, parentNode, list)
    	  
      }
      
      return
      
    }
    
    var newNode : PersonalityInsight = null
    
    var name = el.name.trim()
    if(el.children == null) el.children = new ArrayList[IBMWatsonTrait]()
    
    if("Big 5".equals(name)) {
      newNode = new Big5()
    } else if("root".equals(name)) {
      //nothing
    } else if("Agreeableness".equals(name)) {
    	newNode = new Agreeableness()
    } else if("Conscientiousness".equals(name)) {
      newNode = new Conscientiousness()
    } else if("Emotional range".equals(name)) {
      newNode= new EmotionalRange()
    } else if("Extraversion".equals(name)) {
      newNode = new Extraversion()
    } else if("Openness".equals(name)) {
      newNode = new Openness()
    } else if("Values".equals(name)) {
      newNode = new Values()
    } else if("Needs".equals(name)) {
      newNode = new Needs() 
    }
    
    if(newNode != null) {
      newNode.generateURI(null.asInstanceOf[VitalApp])
      newNode.set(classOf[Property_hasName], name);
      newNode.set(classOf[Property_hasValue], el.percentage)
          
      if(el.category != null) {
        newNode.set(classOf[Property_hasCategory], name)
      }
      list.add(newNode)
    } else {
      
      if(parentNode != null) {
        
        //just set the properties
        val chunks = name.split("[ -]")
        
        var pName = ""
        
        var first = true
        
        for(ch <- chunks) {
          
          if(first) {
            //lowercase
            pName = ch.substring(0, 1).toLowerCase() + ch.substring(1)
            first = false
          } else {
            
            pName += ( ch.substring(0, 1).toUpperCase() + ch.substring(1) )
            
          }
          
        }
        
        pName += "Value"
        
        parentNode.setProperty(pName, el.percentage);
        
      }
      
    }
    
    for( child <- el.children) {
      
      if(parentNode == null || newNode != null) {
        
    	  processTree(child, newNode, list)
    	  
      }
      
    }
    
  }
  
  @Override
  override def getSupportedFeatures() : Collection[Class[_ <: Feature]] = {
    return Arrays.asList(
        classOf[TextFeature],
        //for lang
        classOf[StringFeature]
    )
  }
 
  
  @Override
  override def onResourcesProcessed(): Unit = {
    
//    if(!modelBinaryLoaded))))) throw new Exception("Model was not loaded, make sure " + model_bin + " file is in the model files"
    
  }

  
}