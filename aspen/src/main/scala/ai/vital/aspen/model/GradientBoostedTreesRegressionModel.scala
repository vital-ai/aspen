package ai.vital.aspen.model

import ai.vital.predictmodel.NumericalFeature

object GradientBoostedTreesRegressionModel {
  
  val spark_gradient_boosted_trees_regression = "spark-gradient-boosted-trees-regression";
  
}

@SerialVersionUID(1L)
class GradientBoostedTreesRegressionModel extends GradientBoostedTreesBaseModel {
  
  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = {
    classOf[NumericalFeature]
  }

  def supportedType(): String = {
    GradientBoostedTreesRegressionModel.spark_gradient_boosted_trees_regression
  }
  
}