package ai.vital.aspen.analysis.gradientboostedtrees

import ai.vital.aspen.analysis.training.AbstractTraining
import ai.vital.aspen.model.GradientBoostedTreesPredictionModel
import ai.vital.aspen.util.SetOnceHashMap
import org.apache.spark.rdd.RDD
import ai.vital.aspen.analysis.training.ModelTrainingJob
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.GradientBoostedTrees
import scala.collection.JavaConversions._

class GradientBoostedTreesPredictionTraining(model: GradientBoostedTreesPredictionModel) extends AbstractTraining[GradientBoostedTreesPredictionModel](model) {
  
  def train(globalContext: SetOnceHashMap, trainRDD: RDD[(String, Array[Byte])]): java.io.Serializable = {
    
    val vectorized = ModelTrainingJob.vectorize(trainRDD, model)
    
    //binary supported so far
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = model.numIterations // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.numClasses = 2//model.getTrainedCategories.getCategories.size()
    boostingStrategy.treeStrategy.maxDepth = model.maxDepth
//  Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = model.getCategoricalFeaturesMap().toMap

    val trained = GradientBoostedTrees.train(vectorized, boostingStrategy)
    
    model.model = trained
    
    trained
    
  }
  
}