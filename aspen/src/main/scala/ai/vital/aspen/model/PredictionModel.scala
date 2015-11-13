package ai.vital.aspen.model

import ai.vital.aspen.groovy.modelmanager.AspenModel
import scala.collection.mutable.LinkedHashMap
import ai.vital.vitalsigns.model.GraphObject
import java.io.InputStream
import org.apache.commons.io.IOUtils
import scala.collection.JavaConversions._
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.mllib.tree.model.RandomForestModel
import java.util.List
import ai.vital.vitalsigns.VitalSigns
import ai.vital.vitalsigns.model.property.IProperty
import groovy.lang.GString
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import java.util.Arrays
import ai.vital.vitalsigns.uri.URIGenerator
import java.util.ArrayList
import ai.vital.vitalsigns.model.VITAL_Node
import java.nio.charset.StandardCharsets
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.predictmodel.Feature
import ai.vital.predictmodel.Prediction
import java.util.HashMap
import ai.vital.aspen.groovy.featureextraction.FeatureData
import java.util.Collection
import java.util.Collections
import java.util.Comparator
import java.util.Map.Entry
import scala.collection.JavaConversions._
import ai.vital.aspen.groovy.featureextraction.CategoricalFeatureData
import ai.vital.aspen.groovy.featureextraction.NumericalFeatureData
import ai.vital.aspen.groovy.featureextraction.TextFeatureData
import java.util.Date
import org.apache.spark.mllib.regression.LabeledPoint
import ai.vital.aspen.groovy.featureextraction.WordFeatureData
import ai.vital.vitalsigns.model.VITAL_Category
import ai.vital.predictmodel.Taxonomy
import ai.vital.predictmodel.CategoricalFeature
import ai.vital.predictmodel.BinaryFeature
import ai.vital.predictmodel.NumericalFeature
import ai.vital.predictmodel.WordFeature
import ai.vital.predictmodel.TextFeature
import ai.vital.vitalsigns.model.property.NumberProperty
import ai.vital.vitalsigns.model.property.DateProperty
import ai.vital.vitalsigns.model.property.StringProperty

@SerialVersionUID(1L)
abstract class PredictionModel extends AspenModel {

  val model_bin = "model.bin"; 

  val error_txt = "error.txt";

  var error : String = null;
  
  def ex(msg:String) : Unit = { throw new RuntimeException(msg) }
  
//  val spark_randomforest_prediction = "spark-randomforest-prediction";
  
  def supportedType() : String;
  
  def doPredict(v: Vector) : Double;
  
  def deserializeModel(stream: InputStream) : Unit;
  
  def isClustering() : Boolean = {
    false
  }
  
  def isRegression() : Boolean = {
    false
  }
  
  def setError(_error: String) : Unit =  {
    error = _error
  }
  
  def getError() : String = {
    error
  }
  
  var modelBinaryLoaded = false;
  
  def acceptResource(s: String): Boolean = {
    return s.equals(error_txt) || s.equals(model_bin)
  }

  def onResourcesProcessed(): Unit = {
    
    if(!modelBinaryLoaded) throw new Exception("Model was not loaded, make sure " + model_bin + " file is in the model files")
    
  }

  /*
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
    
    val tn = new TargetNode()
    val app : App = null
    tn.setURI(URIGenerator.generateURI(null, classOf[TargetNode], false))
    
    if(isClustering()) {
      
      tn.setProperty("targetDoubleValue", categoryID.doubleValue())
      tn.setProperty("targetScore", 1D)
      
    } else {
      
    	val label = categoriesMap.get(categoryID)
      tn.setProperty("targetStringValue", label.get)
      tn.setProperty("targetScore", 1D)
      
    }
    
    
    if(objectWithFeature != null) {
    	val edge = new Edge_hasTargetNode()
    	edge.setURI(URIGenerator.generateURI(null, classOf[Edge_hasTargetNode], false))
    	edge.addSource(objectWithFeature).addDestination(tn)
    	return Arrays.asList(tn, edge)
    }
    
    return Arrays.asList(tn)
    
  }
  */

  
  /*
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
  */

  def processResource(s: String, stream: InputStream): Unit = {

    if(s.equals(model_bin)) {
      
      deserializeModel(stream)
      
      modelBinaryLoaded = true
      
    } else if(s.equals(error_txt)) {
      
      error = IOUtils.toString(stream, StandardCharsets.UTF_8.name())
      
    }
    
  }

  def validateConfig(): Unit = {
    if( supportedType().equals( modelConfig.getType ) ) {
      
    } else {
      throw new RuntimeException("Unexpected model type: " + modelConfig.getType )
    }
    
  }
  
  
  
  //assing each feature vector range
  @transient
  var sortedFeaturesData : java.util.List[Entry[String, FeatureData]] = null 
  
  private def initVectorData() : Unit = {
    if(sortedFeaturesData != null) return

    sortedFeaturesData = PredictionModelUtils.getOrderedFeatureData(featuresData)
    
  }
  
  
  def getCategoricalFeaturesMap() : HashMap[Int, Int] = {
    
    initVectorData()
    
    var m = new HashMap[Int, Int](); 
    
    //categorical features are first!
    
    var start = 0;
    
    for(x <- sortedFeaturesData) {
      
    	var len = 1 
    
      if(x.getValue.isInstanceOf[CategoricalFeatureData]) {
        
        m.put(start,  x.getValue.asInstanceOf[CategoricalFeatureData].getCategories.size() )
        
      }
      
      start = start + len
      
    }
    
    return m
    
  } 
  
  def vectorizeLabels(block : VitalBlock, featuresMap : java.util.Map[String, Object]) : LabeledPoint = {
    
    initVectorData();
    
    val fe = getFeatureExtraction
    val f = getModelConfig.getTrainFeature.getFunction
    f.rehydrate(fe, fe, fe)
    var category = f.call(block, featuresMap)
    
    if(category.isInstanceOf[IProperty]) category = category.asInstanceOf[IProperty].rawValue()
    
    var categoryID : java.lang.Double = null;
    
    if(getModelConfig.getTrainFeature.getType.eq(classOf[BinaryFeature])) {
        
      if(!category.isInstanceOf[java.lang.Boolean]) throw new RuntimeException("Expected a Boolean categorical value, got: " + category)
        
      val b = category.asInstanceOf[java.lang.Boolean]
        
      if(b) {
        categoryID = 1
      } else {
      	categoryID = 0
      }
        
    } else if(getModelConfig.getTrainFeature.getType.eq(classOf[CategoricalFeature])) {
        
  	  if(!category.isInstanceOf[VITAL_Category]) throw new RuntimeException("Expected a VITAL_Category node, got: " + category)
    	  
  	  val cn = category.asInstanceOf[VITAL_Category]
    			  
      categoryID = trainedCategories.getCategories.indexOf(cn.getURI)
    			  
      if(categoryID < 0) throw new RuntimeException("No categoryID found for URI: " + cn.getURI)
      
    } else {
      
      categoryID = category.asInstanceOf[Number].doubleValue()
      
    }
    
    
    new LabeledPoint( categoryID, vectorizeNoLabels(block, featuresMap));
    
  }
  
  def vectorizeNoLabels(block : VitalBlock, featuresMap : java.util.Map[String, Object]) : Vector = {
    
    initVectorData();
    
    val extractedFeatures = featuresMap
    
    var totalSize = 0
    
    var start = 0;
    
    val elements = new java.util.ArrayList[(Int, Double)]();
    
    for(x <- sortedFeaturesData) {
      
      val k = x.getKey
      
      var v = extractedFeatures.get(k)
      
      if(v.isInstanceOf[IProperty]) v = v.asInstanceOf[IProperty].unwrapped()
      
      val fd = x.getValue
      
      var len = 1 
      
      if(fd.isInstanceOf[CategoricalFeatureData]) {
        
        if(v != null) {
          
          if(!v.isInstanceOf[VITAL_Category]) throw new RuntimeException("Categorical feature function must return VITAL_Category instances!")
          
          val c = v.asInstanceOf[VITAL_Category]
          
        	val cfd = fd.asInstanceOf[CategoricalFeatureData]
        	
          val d = cfd.getCategories.indexOf(c.getURI).doubleValue();
          
          if(d < 0 ) throw new RuntimeException("Category index not found: " + c.getURI())
          
     			elements.add((start, d))
          
        }
        
      } else if(fd.isInstanceOf[WordFeatureData]) {

        val wfd = fd.asInstanceOf[WordFeatureData]
        
        var nv : Double = 0
        
        if( v != null ) {
          
        	if(v.isInstanceOf[Int] || v.isInstanceOf[java.lang.Integer] || v.isInstanceOf[Long] || v.isInstanceOf[java.lang.Long]) {
        		
        		nv = v.asInstanceOf[Number].doubleValue()
        				
        	} else if(v.isInstanceOf[NumberProperty]) {
        		
        		nv = v.asInstanceOf[NumberProperty].doubleValue()
        				
        	} else {
        		
        		throw new RuntimeException("word feature is expected to return an integer")
        		
        	}
        	
        	elements.add((start, nv))
          
        } 
        
        
      } else if(fd.isInstanceOf[NumericalFeatureData]) {
        
        val nfd = fd.asInstanceOf[NumericalFeatureData]
        
        if(v != null) {
          
        	var nv : Double = 0
        			
          if(v.isInstanceOf[Number]) {
        	  nv = v.asInstanceOf[Number].doubleValue()
          } else if(v.isInstanceOf[NumberProperty]) {
            nv = v.asInstanceOf[NumberProperty].doubleValue()
          } else if(v.isInstanceOf[Date]){
        	  nv = v.asInstanceOf[Date].getTime.doubleValue()
          } else if(v.isInstanceOf[DateProperty]){
        	  nv = v.asInstanceOf[DateProperty].getTime.doubleValue()
        	} else throw new RuntimeException("Unexpected numerical feature value: " + v)
        
          elements.add((start, nv))
          
        }
        
      } else if (fd.isInstanceOf[TextFeatureData]) {
        
        val tfd = fd.asInstanceOf[TextFeatureData]
       
        len = tfd.getDictionary.size()

        if (v != null) {

          var text : String = null
          
          if(v.isInstanceOf[String]) {
            text = v.asInstanceOf[String]
          } else if(v.isInstanceOf[GString]) {
            text = v.asInstanceOf[GString].toString()
          } else if(v.isInstanceOf[StringProperty]) {
            text = v.asInstanceOf[StringProperty].toString()
          }
          

          val words = text.toLowerCase().split("\\s+")

          var index2Value: Map[Int, Double] = Map[Int, Double]()

          for (x <- words) {

            val index = tfd.getDictionary.get(x)

            if (index != null) {

              val intIndex: Int = index.intValue()

              var x = index2Value.getOrElse(intIndex, 0D);
              x = x + 1
              index2Value += (intIndex -> x)

            }

          }

          for (e <- index2Value.entrySet()) {

            elements.add((start + e.getKey, e.getValue))

          }
          
        }
        

      } else throw new RuntimeException("Unhandled feature tytpe: " + fd.getClass.getCanonicalName)

      start = start + len
      
      
    }
    
    totalSize = start
    
    Vectors.sparse(totalSize, elements);
    
  }

//  def _predict(block: VitalBlock, featuresMap : java.util.Map[String, Object] ) : Prediction = {
//
//    featureExtraction.ex    
//    
//	  return null;
//  }
  
  @Override
  def _predict(vitalBlock : VitalBlock, featuresMap : java.util.Map[String, Object]) : Prediction = {
    
    val objects : java.util.List[GraphObject] = null
    
    val categoryID = doPredict(vectorizeNoLabels(vitalBlock, featuresMap))
    
    if(modelConfig.getTrainFeature.getType.equals(classOf[BinaryFeature])) {
      
      val p = new BinaryPrediction()
      
      p.binaryValue = categoryID > 0
      
      return p
      
    } else if(modelConfig.getTrainFeature.getType.equals(classOf[NumericalFeature])) {
      
      //regression
      val p = new RegressionPrediction()
      p.value = categoryID
      
      return p
      
    }
    
    val categoryURI = trainedCategories.getCategories.get(categoryID.intValue())
    
    val taxonomyName = modelConfig.getTrainFeature.getTaxonomy
    
    
    val pred = new CategoryPrediction
    pred.categoryURI = categoryURI
    pred.categoryID = categoryID.intValue()
    
    if(taxonomyName != null) {
    
      var x : Taxonomy = null
      
      for(t <- modelConfig.getTaxonomies) {
        if(t.getProvides.equals(taxonomyName)) {
          pred.category = t.getContainer.get(categoryURI).asInstanceOf[VITAL_Category]
        }
      }
      
    }
    
    return pred
    
  } 
  
  
  
  @Override
  override def getSupportedFeatures() : Collection[Class[_ <: Feature]] = {
	  return Arrays.asList(
//        classOf[BinaryFeature],
        classOf[CategoricalFeature],
        classOf[NumericalFeature],
        classOf[TextFeature],
        classOf[WordFeature]
    )
  }
}
