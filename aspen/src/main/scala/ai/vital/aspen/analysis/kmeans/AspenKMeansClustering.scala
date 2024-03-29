package ai.vital.aspen.analysis.kmeans

import ai.vital.aspen.analysis.training.AbstractTraining
import ai.vital.aspen.model.AspenKMeansPredictionModel
import org.apache.spark.rdd.RDD
import ai.vital.aspen.util.SetOnceHashMap
import java.io.Serializable
import ai.vital.aspen.analysis.training.ModelTrainingJob
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeans

class AspenKMeansClusteringTraining(model: AspenKMeansPredictionModel) extends AbstractTraining[AspenKMeansPredictionModel](model) {
  
  def train(globalContext: java.util.Map[String, Object], trainRDD: RDD[(String, Array[Byte])]): Serializable = {
   
    val parsedData = ModelTrainingJob.vectorizeNoLabels(trainRDD, model)
    
    parsedData.cache()
          
    val clusters = KMeans.train(parsedData, model.clustersCount, model.numIterations)
          
    model.setModel(clusters)
          
     // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    val msg = "Within Set Sum of Squared Errors = " + WSSSE
    
    println(msg)
    model.setError(msg)
          
    return clusters
    
  }

  
}