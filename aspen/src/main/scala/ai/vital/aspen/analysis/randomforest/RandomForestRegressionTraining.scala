package ai.vital.aspen.analysis.randomforest

import ai.vital.aspen.analysis.training.AbstractTraining
import ai.vital.aspen.model.RandomForestRegressionModel
import ai.vital.aspen.util.SetOnceHashMap
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD
import ai.vital.aspen.analysis.training.ModelTrainingJob
import org.apache.spark.mllib.tree.RandomForest

class RandomForestRegressionTraining(model:RandomForestRegressionModel) extends AbstractTraining[RandomForestRegressionModel](model) {
  
  def train(globalContext: SetOnceHashMap, trainRDD: RDD[(String, Array[Byte])]): java.io.Serializable = {

    val vectorized = ModelTrainingJob.vectorize(trainRDD, model);
          
    val categoricalFeaturesInfo = model.getCategoricalFeaturesMap()
    val numTrees = model.numTrees
    val featureSubsetStrategy = model.featureSubsetStrategy
    val impurity = model.impurity
    val maxDepth = model.maxDepth
    val maxBins = model.maxBins

    val trained = RandomForest.trainRegressor(vectorized, categoricalFeaturesInfo.toMap,
              numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
              
    model.setModel(trained)
          
    return trained
    
  }
  
}