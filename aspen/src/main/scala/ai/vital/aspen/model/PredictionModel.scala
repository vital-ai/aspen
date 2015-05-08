package ai.vital.aspen.model

import ai.vital.aspen.groovy.modelmanager.AspenModel
import scala.collection.mutable.LinkedHashMap
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.domain.TargetNode
import java.io.InputStream
import org.apache.commons.io.IOUtils
import scala.collection.JavaConversions._
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.mllib.tree.model.RandomForestModel
import java.util.List
import ai.vital.vitalsigns.VitalSigns
import ai.vital.property.IProperty
import groovy.lang.GString
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import java.util.Arrays
import ai.vital.common.uri.URIGenerator
import java.util.ArrayList
import ai.vital.domain.Edge_hasTargetNode
import ai.vital.vitalsigns.model.VITAL_Node

@SerialVersionUID(1L)
abstract class PredictionModel extends AspenModel {

  val categories_tsv = "categories.tsv";
  
  val dictionary_tsv = "dictionary.tsv";
  
  val model_bin = "model.bin"; 
  
//  val spark_randomforest_prediction = "spark-randomforest-prediction";
  
  var categoriesMap : LinkedHashMap[Int, String] = null;//new LinkedHashMap[Int, String]()
  var dictionaryMap : LinkedHashMap[String, Int] = null;//new LinkedHashMap[String, Int]()
  
//  var model : RandomForestModel = null;
  
  var features : LinkedHashMap[Class[GraphObject], List[String]] = null
  
  def supportedType() : String;
  
  def doPredict(v: Vector) : Int;
  
  def deserializeModel(stream: InputStream) : Unit;
  
  var modelBinaryLoaded = false;
  
  def acceptResource(s: String): Boolean = {
    return s.equals(categories_tsv) || s.equals(dictionary_tsv) || s.equals(model_bin)
  }

  def onResourcesProcessed(): Unit = {
    
    if(categoriesMap == null) throw new Exception("Categories map was not loaded, make sure " + categories_tsv + " file is in the model files")
    if(categoriesMap.size < 2) throw new Exception("Categories map size must be greater than 1")
    
    if(dictionaryMap == null) throw new Exception("Dictionary map was not loaded, make sure " + dictionary_tsv + " file is in the model files")
    if(dictionaryMap.size < 1) throw new Exception("Dictionary map cannot be empty")
    
    
    if(!modelBinaryLoaded) throw new Exception("Model was not loaded, make sure " + model_bin + " file is in the model files")
    
  }

  def predict(input: List[GraphObject]): List[GraphObject] = {
    
    val text = new StringBuilder()
    
    var objectWithFeature : VITAL_Node = null;
    
    for(g <- input) {
      
      for(e <- features.entrySet()) {
        
        if(e.getKey.isAssignableFrom(g.getClass)) {
          
          
          for(pn <- e.getValue) {
            
        	  val pv = g.getProperty(pn)
            
        	  if(pv != null) {
        		  
        		  val p : String = pv match {
        		  case x: IProperty => "" + x.rawValue()
        		  case y: String => y
        		  case z: GString => z.toString()
        		  case _ => throw new Exception("Cannot get string value from property " + pv)
        	  }
        	  
        	  if(text.length > 0) {
        		  text.append("\n")
        	  }
        	  
        	  text.append(p)
        	  
        	  objectWithFeature = g match {
          	  case x: VITAL_Node => x
          	  case _ => null
          	  }
        	  
        	  } 
            
          }
          
        }
        
      }
      
    }
    
    if(text.length() < 1) return new ArrayList[GraphObject]()
    
    val v = vectorize(text.toString())
    
//    val category = model.predict(v)
      
    var categoryID = doPredict(v);
    
    val label = categoriesMap.get(categoryID)
    
    val tn = new TargetNode()
    val app : App = null
    tn.setURI(URIGenerator.generateURI(null, classOf[TargetNode], false))
    
    tn.setProperty("targetStringValue", label.get)
    tn.setProperty("targetScore", 1D)
    
    if(objectWithFeature != null) {
    	val edge = new Edge_hasTargetNode()
    	edge.setURI(URIGenerator.generateURI(null, classOf[Edge_hasTargetNode], false))
    	edge.addSource(objectWithFeature).addDestination(tn)
    	return Arrays.asList(tn, edge)
    }
    
    return Arrays.asList(tn)
    
  }
  
  def vectorize(str: String) : Vector = {
    
      var index2Value: Map[Int, Double] = Map[Int, Double]()

      val words = str.toLowerCase().split("\\s+")

      for (x <- words) {

        val index = dictionaryMap.getOrElse(x, -1)

        if (index >= 0) {

          var v = index2Value.getOrElse(index, 0D);
          v = v + 1
          index2Value += (index -> v)

        }

      }

      val s = index2Value.toSeq.sortWith({ (p1, p2) =>
        p1._1 < p2._1
      })

      val v = Vectors.sparse(dictionaryMap.size, s)
      
      return v

  }

  def processResource(s: String, stream: InputStream): Unit = {

    if(s.equals(categories_tsv)) {

      categoriesMap = new LinkedHashMap[Int, String]()
      
      for(catLine <- IOUtils.readLines(stream, "UTF-8")) {
        if(!catLine.isEmpty()) {
          val s = catLine.split("\t")
          categoriesMap.put(Integer.parseInt(s(0)), s(1))
        }
      }
      
    }
    
    if(s.equals(dictionary_tsv)) {
      
      dictionaryMap = new LinkedHashMap[String, Int]()
      
      for(dictLine <- IOUtils.readLines(stream, "UTF-8")) {
        if(!dictLine.isEmpty()) {
          val s = dictLine.split("\t")
          dictionaryMap.put(s(1), Integer.parseInt(s(0)))
        }
      }
      
    }
    
    if(s.equals(model_bin)) {
      
      deserializeModel(stream)
      
      modelBinaryLoaded = true
      
    }

  }

  def validateConfig(): Unit = {
    if( supportedType().equals( modelConfig.getType ) ) {
      
    } else {
      throw new RuntimeException("Unexpected model type: " + modelConfig.getType )
    }
    
        //prepare features
    
    features = new Features().parseFeaturesMap(this)
    
    if(features.size < 1) throw new Exception("No features defined in the model")
    
  }

  
}