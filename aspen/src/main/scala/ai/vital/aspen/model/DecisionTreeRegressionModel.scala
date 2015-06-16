package ai.vital.aspen.model

import ai.vital.predictmodel.NumericalFeature

object DecisionTreeRegressionModel {
  
  val spark_decision_tree_regression = "spark-decision-tree-regression";
  
}


@SerialVersionUID(1L)
class DecisionTreeRegressionModel extends DecisionTreeBaseModel {
  
  def getDefaultImpurity(): String = {
    "variance"
  }

  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = {
    classOf[NumericalFeature]
  }

  def supportedType(): String = {
    DecisionTreeRegressionModel.spark_decision_tree_regression
  }
}