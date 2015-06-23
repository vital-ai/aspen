package ai.vital.aspen.analysis.decisiontree

import ai.vital.aspen.analysis.training.AbstractTraining
import scala.collection.JavaConversions._
import ai.vital.aspen.model.AspenDecisionTreePredictionModel
import ai.vital.aspen.util.SetOnceHashMap
import org.apache.spark.rdd.RDD
import java.io.Serializable
import ai.vital.aspen.analysis.training.ModelTrainingJob
import org.apache.spark.mllib.tree.DecisionTree

class AspenDecisionTreeTraining(model: AspenDecisionTreePredictionModel) extends AbstractTraining[AspenDecisionTreePredictionModel](model) {
  
  def train(globalContext: java.util.Map[String, Object], trainRDD: RDD[(String, Array[Byte])]): Serializable = {

    val vectorized = ModelTrainingJob.vectorize(trainRDD, model);
          
    val numClasses = model.getTrainedCategories.getCategories.size()
    val categoricalFeaturesInfo = model.getCategoricalFeaturesMap()
    val impurity = model.impurity
    val maxDepth = model.maxDepth
    val maxBins = model.maxBins
          
    val trained = DecisionTree.trainClassifier(vectorized, numClasses, categoricalFeaturesInfo.toMap, impurity,
            maxDepth, maxBins)
        
    model.setModel(trained)

    return trained

  }
}