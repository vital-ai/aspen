package ai.vital.aspen.analysis.decisiontree

import ai.vital.aspen.analysis.training.AbstractTraining
import ai.vital.aspen.model.DecisionTreeRegressionModel
import ai.vital.aspen.util.SetOnceHashMap
import org.apache.spark.rdd.RDD
import ai.vital.aspen.analysis.training.ModelTrainingJob
import org.apache.spark.mllib.tree.DecisionTree
import scala.collection.JavaConversions._

class DecisionTreeRegressionTraining(model: DecisionTreeRegressionModel) extends AbstractTraining[DecisionTreeRegressionModel](model) {
  
  def train(globalContext: SetOnceHashMap, trainRDD: RDD[(String, Array[Byte])]): java.io.Serializable = {
    
    val vectorized = ModelTrainingJob.vectorize(trainRDD, model);
          
    val categoricalFeaturesInfo = model.getCategoricalFeaturesMap().toMap
    val impurity = model.impurity
    val maxDepth = model.maxDepth
    val maxBins = model.maxBins
          
    val trained = DecisionTree.trainRegressor(vectorized, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
        
    model.setModel(trained)

    return trained
  }
  
}