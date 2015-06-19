package ai.vital.aspen.analysis.regression

import ai.vital.aspen.analysis.training.AbstractTraining
import ai.vital.aspen.model.AspenIsotonicRegressionModel
import ai.vital.aspen.util.SetOnceHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.IsotonicRegression
import ai.vital.aspen.analysis.training.ModelTrainingJob

class AspenIsotonicRegressionTraining(model: AspenIsotonicRegressionModel) extends AbstractTraining[AspenIsotonicRegressionModel](model) {

  def train(globalContext: SetOnceHashMap, trainRDD: RDD[(String, Array[Byte])]): java.io.Serializable = {
  
    val vectorized = ModelTrainingJob.vectorize(trainRDD, model)
    
    //convert to (label,value,weight) tuples
    
    val training = model.toTuple(vectorized)
    
    training.cache()
    
    
    // Create isotonic regression model from training data.
    // Isotonic parameter defaults to true so it is only shown for demonstration
    
    val trained= new IsotonicRegression().setIsotonic(model.isotonic).run(training)
    
    model.model = trained
    
    return trained
    
  }
  
}