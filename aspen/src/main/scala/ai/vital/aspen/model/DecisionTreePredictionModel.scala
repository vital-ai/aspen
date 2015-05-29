package ai.vital.aspen.model

import java.io.InputStream
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.commons.io.IOUtils

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
  
  
}