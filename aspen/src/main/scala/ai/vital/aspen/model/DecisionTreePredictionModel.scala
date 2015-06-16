package ai.vital.aspen.model

import java.io.File
import java.io.InputStream
import java.io.Serializable
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
import ai.vital.predictmodel.CategoricalFeature

object DecisionTreePredictionModel {
  
	val spark_decision_tree_prediction = "spark-decision-tree-prediction";
  
}

@SerialVersionUID(1L)
class DecisionTreePredictionModel extends DecisionTreeBaseModel {

  def supportedType(): String = {
    return DecisionTreePredictionModel.spark_decision_tree_prediction
  }
  
  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = {
    classOf[CategoricalFeature]
  }

  def getDefaultImpurity(): String = {
     "gini"
  }
  
}