package ai.vital.aspen.analysis.regression

import ai.vital.aspen.analysis.training.AbstractTraining
import ai.vital.aspen.model.SparkLinearRegressionModel
import ai.vital.aspen.util.SetOnceHashMap
import org.apache.spark.rdd.RDD
import ai.vital.aspen.analysis.training.ModelTrainingJob
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.RidgeRegressionWithSGD
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.mllib.regression.LassoWithSGD

class SparkLinearRegressionTraining(model: SparkLinearRegressionModel) extends AbstractTraining[SparkLinearRegressionModel](model) {

  def train(globalContext: SetOnceHashMap, trainRDD: RDD[(String, Array[Byte])]): java.io.Serializable = {

   val vectorized = ModelTrainingJob.vectorizeWithScaling(trainRDD, model);
          
   
   vectorized.cache()
   
   val numIterations = model.numIterations
      
    //normalize

   var trained : GeneralizedLinearModel = null 
   
   
   val alg = model.getModelConfig.getAlgorithm
   
   if(SparkLinearRegressionModel.algorithm_lasso_with_sgd.equals(alg)) {

     trained = LassoWithSGD.train(vectorized, numIterations)
     
   } else if(SparkLinearRegressionModel.algorithm_linear_regression_with_sgd.equals(alg)) {
     
	   trained = LinearRegressionWithSGD.train(vectorized, numIterations)
     
   } else if(SparkLinearRegressionModel.algorithm_ridge_regression_with_sgd.equals(alg)) {
     
	   trained = RidgeRegressionWithSGD.train(vectorized, numIterations)
     
   } else {
     throw new RuntimeException("unknown algorithm: " + alg)
   }
   
          
    model.setModel(trained)
          
    return trained
    
  }
}