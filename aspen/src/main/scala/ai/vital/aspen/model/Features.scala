package ai.vital.aspen.model

import ai.vital.aspen.groovy.modelmanager.AspenModel
import scala.collection.mutable.LinkedHashMap
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.VitalSigns
import scala.collection.JavaConversions._
import java.util.List
import java.util.ArrayList

/**
 * Simple features analyzer
 */
class Features {

  /*
  def parseFeaturesMap(model: AspenModel) : LinkedHashMap[Class[GraphObject], List[String]] = {
   
    val modelConfig = model.getModelConfig;
    
    val features : LinkedHashMap[Class[GraphObject], List[String]] = new LinkedHashMap[Class[GraphObject], List[String]]();
    
    for( feature <- modelConfig.getFeatures ) {
      
      if(!"text".equals(feature.getType)) {
        throw new RuntimeException("Only 'text' features supported in " + this.getClass.getSimpleName + " class")
      }
      
      val fv = feature.getValue
      
      val lastDot = fv.lastIndexOf('.')

      if(lastDot <= 0 || lastDot == fv.length() -1 ) {
        throw new Exception("Invalid feature value: " + fv)
      }
      
      val cls = Class.forName(fv.substring(0, lastDot))
      
      if(classOf[GraphObject].isAssignableFrom(cls)) {
      } else {
        
      }

      val c: Class[GraphObject] = cls match {
        case x: Class[GraphObject]  => x
        case _ => throw new Exception("Class " + cls + " is not a subclass of GraphObject")
      }

      
      val pname = fv.substring(lastDot + 1)
      
      val vsProp = VitalSigns.get().getPropertiesRegistry.getPropertyByShortName(c, pname)
      
      if(vsProp == null) throw new Exception("Property not found or ambiguous: " + pname)
      
      var l = features.getOrElse(c, null);
      
      if(l == null) {
        l = new ArrayList[String]()
        features.put(c, l)
      }
      
      l.add(pname)
      
      
    }
    
    return features
    
  }
  * */
  
}