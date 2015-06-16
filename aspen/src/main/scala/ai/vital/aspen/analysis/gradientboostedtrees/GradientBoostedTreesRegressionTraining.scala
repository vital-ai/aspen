package ai.vital.aspen.analysis.gradientboostedtrees

import ai.vital.aspen.analysis.training.AbstractTraining
import ai.vital.aspen.model.GradientBoostedTreesRegressionModel
import org.apache.spark.rdd.RDD
import ai.vital.aspen.util.SetOnceHashMap
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.GradientBoostedTrees
import ai.vital.aspen.analysis.training.ModelTrainingJob
import scala.collection.JavaConversions._

class GradientBoostedTreesRegressionTraining(model: GradientBoostedTreesRegressionModel) extends AbstractTraining[GradientBoostedTreesRegressionModel](model) {
  
  def train(globalContext: SetOnceHashMap, trainRDD: RDD[(String, Array[Byte])]): java.io.Serializable = {

    val vectorized = ModelTrainingJob.vectorize(trainRDD, model)
    
    vectorized.cache()
    
    // Train a GradientBoostedTrees model.
    //  The defaultParams for Regression use SquaredError by default.
    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.numIterations = model.numIterations // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.maxDepth = model.maxDepth
//  Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = model.getCategoricalFeaturesMap().toMap

    val trained = GradientBoostedTrees.train(vectorized, boostingStrategy)
    
    model.model = trained
    
    trained
    
  }
}