package ai.vital.aspen.analysis.randomforest

import java.util.HashMap
import scala.collection.JavaConversions._
import org.apache.spark.mllib.tree.RandomForest
import ai.vital.aspen.analysis.training.AbstractTraining
import ai.vital.aspen.model.AspenRandomForestPredictionModel
import ai.vital.aspen.util.SetOnceHashMap
import org.apache.spark.rdd.RDD
import ai.vital.aspen.analysis.training.ModelTrainingJob

class AspenRandomForestTraining(model: AspenRandomForestPredictionModel) extends AbstractTraining[AspenRandomForestPredictionModel](model) {
  
  def train(globalContext: SetOnceHashMap, trainRDD: RDD[(String, Array[Byte])]): java.io.Serializable = {
    
    val vectorized = ModelTrainingJob.vectorize(trainRDD, model);
          
    val numClasses = model.getTrainedCategories.getCategories.size()
    val categoricalFeaturesInfo = model.getCategoricalFeaturesMap()
    val numTrees = model.numTrees
    val featureSubsetStrategy = model.featureSubsetStrategy
    val impurity = model.impurity
    val maxDepth = model.maxDepth 
    val maxBins = model.maxBins
          
    val trained = RandomForest.trainClassifier(vectorized, numClasses, categoricalFeaturesInfo.toMap,
              numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
              
    model.setModel(trained)
          
    return trained
    
  }
}