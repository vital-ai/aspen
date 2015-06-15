package ai.vital.aspen.analysis.decisiontree

import ai.vital.aspen.analysis.training.AbstractTraining
import scala.collection.JavaConversions._
import ai.vital.aspen.model.DecisionTreePredictionModel
import ai.vital.aspen.util.SetOnceHashMap
import org.apache.spark.rdd.RDD
import java.io.Serializable
import ai.vital.aspen.analysis.training.ModelTrainingJob
import org.apache.spark.mllib.tree.DecisionTree

class DecisionTreeTraining(model: DecisionTreePredictionModel) extends AbstractTraining[DecisionTreePredictionModel](model) {
  
  def train(globalContext: SetOnceHashMap, trainRDD: RDD[(String, Array[Byte])]): Serializable = {

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