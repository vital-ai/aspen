package ai.vital.aspen.model

import ai.vital.predictmodel.NumericalFeature

object AspenDecisionTreeRegressionModel {
  
  val spark_decision_tree_regression = "spark-decision-tree-regression";
  
}


@SerialVersionUID(1L)
class AspenDecisionTreeRegressionModel extends AspenDecisionTreeBaseModel {
  
  def getDefaultImpurity(): String = {
    "variance"
  }

  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = {
    classOf[NumericalFeature]
  }

  def supportedType(): String = {
    AspenDecisionTreeRegressionModel.spark_decision_tree_regression
  }
}