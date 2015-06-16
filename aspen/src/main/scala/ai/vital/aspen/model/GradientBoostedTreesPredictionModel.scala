package ai.vital.aspen.model

import ai.vital.predictmodel.BinaryFeature

object GradientBoostedTreesPredictionModel {
  
  val spark_gradient_boosted_trees_prediction = "spark-gradient-boosted-trees-prediction";
  
}

@SerialVersionUID(1L)
class GradientBoostedTreesPredictionModel extends GradientBoostedTreesBaseModel {
  
  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = {
    classOf[BinaryFeature]
  }

  def supportedType(): String = {
    GradientBoostedTreesPredictionModel.spark_gradient_boosted_trees_prediction
  }
  
}