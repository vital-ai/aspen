package ai.vital.aspen.analysis.gaussianmixture

import ai.vital.aspen.analysis.training.AbstractTraining
import org.apache.spark.rdd.RDD
import ai.vital.aspen.util.SetOnceHashMap
import org.apache.spark.mllib.clustering.GaussianMixture
import ai.vital.aspen.analysis.training.ModelTrainingJob
import ai.vital.aspen.model.AspenGaussianMixturePredictionModel

class AspenGaussianMixtureTraining(model: AspenGaussianMixturePredictionModel) extends AbstractTraining[AspenGaussianMixturePredictionModel](model) {
  
  def train(globalContext: SetOnceHashMap, trainRDD: RDD[(String, Array[Byte])]): java.io.Serializable = {

    val vectorized = ModelTrainingJob.vectorizeNoLabels(trainRDD, model)
    
    vectorized.cache()
    
    // Cluster the data into two classes using GaussianMixture
    val gmm = new GaussianMixture().setK(model.k).run(vectorized)
    
    model.model = gmm
    
    return gmm
    
  }
  
}