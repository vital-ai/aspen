package ai.vital.aspen.analysis.regression

import ai.vital.aspen.analysis.training.AbstractTraining
import ai.vital.aspen.model.SparkLinearRegressionModel
import ai.vital.aspen.util.SetOnceHashMap
import org.apache.spark.rdd.RDD
import ai.vital.aspen.analysis.training.ModelTrainingJob
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

class SparkLinearRegressionTraining(model: SparkLinearRegressionModel) extends AbstractTraining[SparkLinearRegressionModel](model) {

  def train(globalContext: SetOnceHashMap, trainRDD: RDD[(String, Array[Byte])]): java.io.Serializable = {

   val vectorized = ModelTrainingJob.vectorizeWithScaling(trainRDD, model);
          
   val numIterations = model.numIterations
      
    //normalize

    val trained = LinearRegressionWithSGD.train(vectorized, numIterations)
          
    model.setModel(trained)
          
    return trained
    
  }
}