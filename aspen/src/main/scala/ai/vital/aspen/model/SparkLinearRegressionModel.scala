package ai.vital.aspen.model

import org.apache.spark.mllib.linalg.Vector
import java.io.InputStream
import org.apache.commons.lang3.SerializationUtils
import org.apache.commons.io.IOUtils
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.predictmodel.Prediction
import ai.vital.vitalsigns.model.GraphObject
import java.io.File
import java.io.FileOutputStream
import org.apache.commons.io.FileUtils
import java.nio.charset.StandardCharsets
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.StandardScaler
import scala.collection.mutable.MutableList
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.feature.StandardScalerModel
import org.apache.spark.mllib.linalg.SparseVector

object SparkLinearRegressionModel {
  
  val spark_linear_regression = "spark-linear-regression";
}

@SerialVersionUID(1L)
class SparkLinearRegressionModel extends PredictionModel {

  var model : LinearRegressionModel = null
  
  var labelsScaler : StandardScalerModel = null
  //labels scaling function factors y = ax + b
  var a = -1d
  var b = -1d
  
  var scaler : StandardScalerModel = null
  
  
  def setModel(_model: LinearRegressionModel) : Unit = {
    model = _model
  }
  
  def getModel() : LinearRegressionModel = {
    model
  }
  
  def deserializeModel(stream: InputStream): Unit = {

      val deserializedModel : LinearRegressionModel = SerializationUtils.deserialize(IOUtils.toByteArray(stream))
    
      model = deserializedModel match {
        case x: LinearRegressionModel => x
        case _ => throw new ClassCastException
      }
  }

  def doPredict(v: Vector): Int = {
     throw new RuntimeException("shouldn't be called")
  }
  
  @Override
  override def _predict(vitalBlock : VitalBlock, featuresMap : java.util.Map[String, Object]) : Prediction = {
    
    val objects : java.util.List[GraphObject] = null
    
    val notScaled = vectorizeNoLabels(vitalBlock, featuresMap)
    
    var value = model.predict(scaleVector(notScaled))
    
    //scale it back
    
    value = ( value - b ) / a
    
    val pred = new RegressionPrediction
    pred.value = value
    
    return pred
    
  }
  
  
  def vectorizeLabelsNoScaling(vitalBlock: VitalBlock, featuresMap : java.util.Map[String, Object]) : LabeledPoint = {
   super.vectorizeLabels(vitalBlock, featuresMap)
  }

  def isCategorical(): Boolean = {
    false
  }

  @Override
  def isTestedWithTrainData() : Boolean = {
    return true;
  }

  def supportedType(): String = {
    SparkLinearRegressionModel.spark_linear_regression
  }

  def persistFiles(tempDir: File): Unit = {
    
    val os = new FileOutputStream(new File(tempDir, model_bin))
    SerializationUtils.serialize(model, os)
    os.close()
    
    if(error != null) {
      FileUtils.writeStringToFile(new File(tempDir, error_txt), error, StandardCharsets.UTF_8.name())
    }
    
  }
  
  
  def initScaler(vectorized : RDD[LabeledPoint]) : Unit = {
 
    if(scaler != null) throw new RuntimeException("Scaler already initialized!")
    
    val vectors = vectorized.map { lp =>
      
//      var vals = MutableList[Double]()
//      vals += lp.label
//      for( v <- lp.features.toArray ) {
//        vals += v
//      }
//      Vectors.dense(vals.toArray)
      
      lp.features
      
    }
    
    scaler = new StandardScaler(true, true).fit(vectors)
    
    
    
    val labelsVectors = vectorized.map { lp =>
      Vectors.dense(lp.label)
    }

    labelsScaler = new StandardScaler(true, true).fit(labelsVectors)
    
 
    val labelsVals = labelsVectors.map { x => x(0) }
    
    val minV = labelsVals.min()  //x1
    
    val maxV = labelsVals.max() //x2
    
    val minVS = labelsScaler.transform(Vectors.dense(minV))(0) //y1
    val maxVS = labelsScaler.transform(Vectors.dense(maxV))(0) //y2
    
    //solve linear equation 
    a = (maxVS - minVS) / (maxV-minV)
    b = minVS - a * minV
//  b = maxVS - a * maxV
    
    
    
  }
  
  /*
  override def vectorizeLabels(block : VitalBlock, featuresMap : java.util.Map[String, Object]) : LabeledPoint = {
  
    if(scaler == null) throw new RuntimeException("Scaler not initialized")
    
    val lp = super.vectorizeLabels(block, featuresMap)
  
    var features = lp.features
    
    if(features.isInstanceOf[SparseVector]) {
      features = Vectors.dense(features.toArray)
    }
    
    LabeledPoint(labelsScaler.transform(Vectors.dense(lp.label))(0), scaler.transform(features))
    
  }
  
  override def vectorizeNoLabels(block : VitalBlock, featuresMap : java.util.Map[String, Object]) : Vector = {
    
//		if(scaler == null) throw new RuntimeException("Scaler not initialized")
    
    var notScaled = super.vectorizeNoLabels(block, featuresMap)

    if(scaler == null) return notScaled
    
    if(notScaled.isInstanceOf[SparseVector]) {
      notScaled = Vectors.dense(notScaled.toArray)
    }
    
    scaler.transform(notScaled)
  
  }
  */
  def scaleLabeledPoint(lp : LabeledPoint) : LabeledPoint = {
  
    if(scaler == null) throw new RuntimeException("Scaler not initialized")
    
    var features = lp.features
    
    if(features.isInstanceOf[SparseVector]) {
      features = Vectors.dense(features.toArray)
    }
    
    LabeledPoint(labelsScaler.transform(Vectors.dense(lp.label))(0), scaler.transform(features))
    
  }
  
  
  def scaleVector(v : Vector) : Vector = {
		  
		  if(scaler == null) throw new RuntimeException("Scaler not initialized")
		  
		  var features = v
		  
		  if(features.isInstanceOf[SparseVector]) {
			  features = Vectors.dense(features.toArray)
		  }
		  
		  scaler.transform(features)
		  
  }
  
  def scaledBack(prediction: Double) : Double = {
 
		if(scaler == null) throw new RuntimeException("Scaler not initialized")

    return ( prediction - b ) / a
  }
}