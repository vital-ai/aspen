package ai.vital.aspen.analysis.logisticregression

import ai.vital.aspen.analysis.training.AbstractTraining
import ai.vital.aspen.model.AspenLogisticRegressionPredictionModel
import org.apache.spark.rdd.RDD
import ai.vital.aspen.util.SetOnceHashMap
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import ai.vital.aspen.analysis.training.ModelTrainingJob

class AspenLogisticRegressionPredictionTraining(model: AspenLogisticRegressionPredictionModel) extends AbstractTraining[AspenLogisticRegressionPredictionModel](model) {
  
  def train(globalContext: SetOnceHashMap, trainRDD: RDD[(String, Array[Byte])]): java.io.Serializable = {
    
    // Run training algorithm to build the model
    val vectorized = ModelTrainingJob.vectorize(trainRDD, model)
    
    val trained = new LogisticRegressionWithLBFGS()
      .setNumClasses(model.getTrainedCategories.getCategories.size())
      .run(vectorized)
      
    model.model= trained
      
    trained
    
  }
  
}