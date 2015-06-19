package ai.vital.aspen.model

import ai.vital.predictmodel.BinaryFeature

object AspenGradientBoostedTreesPredictionModel {
  
  val spark_gradient_boosted_trees_prediction = "spark-gradient-boosted-trees-prediction";
  
}

@SerialVersionUID(1L)
class AspenGradientBoostedTreesPredictionModel extends AspenGradientBoostedTreesBaseModel {
  
  def getTrainFeatureType(): Class[_ <: ai.vital.predictmodel.Feature] = {
    classOf[BinaryFeature]
  }

  def supportedType(): String = {
    AspenGradientBoostedTreesPredictionModel.spark_gradient_boosted_trees_prediction
  }
  
}