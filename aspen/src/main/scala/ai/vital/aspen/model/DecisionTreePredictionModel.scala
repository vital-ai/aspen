package ai.vital.aspen.model

import java.io.File
import java.io.InputStream
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.commons.io.IOUtils
import java.io.FileOutputStream
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.predictmodel.Prediction
import ai.vital.aspen.groovy.modelmanager.AspenPrediction
import ai.vital.vitalsigns.model.GraphObject
import org.apache.commons.io.FileUtils
import java.nio.charset.StandardCharsets

object DecisionTreePredictionModel {
  
	val spark_decision_tree_prediction = "spark-decision-tree-prediction";
  
}

@SerialVersionUID(1L)
class DecisionTreePredictionModel extends PredictionModel {

  var model : DecisionTreeModel = null;
  
  def supportedType(): String = {
    return DecisionTreePredictionModel.spark_decision_tree_prediction
  }
  
  def setModel(_model: DecisionTreeModel) : Unit = {
    model = _model
  }
  
  def getModel() : DecisionTreeModel = {
    model
  }

  def deserializeModel(stream: InputStream): Unit = {
    
      val deserializedModel : DecisionTreeModel = SerializationUtils.deserialize(IOUtils.toByteArray(stream))
    
      model = deserializedModel match {
        case x: DecisionTreeModel => x
        case _ => throw new ClassCastException
      }
      
  }

  def doPredict(v: Vector): Int = {
    return model.predict(v).intValue()
  }
 
  @Override
  def persistFiles(tempDir : File) : Unit = {

    val os = new FileOutputStream(new File(tempDir, model_bin))
    SerializationUtils.serialize(model, os)
    os.close()
    
    if(error != null) {
      FileUtils.writeStringToFile(new File(tempDir, error_txt), error, StandardCharsets.UTF_8.name())
    }
	  
  }


  @Override
  def isTestedWithTrainData() : Boolean = {
  	return false;
  }
  
  @Override
  def isCategorical() : Boolean = {
		  return true;
  }
  
}