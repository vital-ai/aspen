package ai.vital.aspen.model

import ai.vital.predictmodel.NumericalFeature

object AspenGradientBoostedTreesRegressionModel {
  
  val spark_gradient_boosted_trees_regression = "spark-gradient-boosted-trees-regression";
  
}

@SerialVersionUID(1L)
class AspenGradientBoostedTreesRegressionModel extends AspenGradientBoostedTreesBaseModel {
  
  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = {
    classOf[NumericalFeature]
  }

  def supportedType(): String = {
    AspenGradientBoostedTreesRegressionModel.spark_gradient_boosted_trees_regression
  }
  
}