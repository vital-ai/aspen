package ai.vital.aspen.analysis.naivebayes

import ai.vital.aspen.model.AspenNaiveBayesPredictionModel
import ai.vital.aspen.analysis.training.AbstractTraining
import org.apache.spark.rdd.RDD
import java.io.Serializable
import ai.vital.aspen.util.SetOnceHashMap
import ai.vital.aspen.analysis.training.ModelTrainingJob
import org.apache.spark.mllib.classification.NaiveBayes

class AspenNaiveBayesTraining(model : AspenNaiveBayesPredictionModel) extends AbstractTraining[AspenNaiveBayesPredictionModel](model) {
  
  def train(globalContext: SetOnceHashMap, trainRDD: RDD[(String, Array[Byte])]): Serializable = {

    val vectorized = ModelTrainingJob.vectorize(trainRDD, model);
          
    val trained = NaiveBayes.train(vectorized, model.lambda)
          
    model.setModel(trained)
          
    return trained 
    
  }
  
}