package ai.vital.aspen.analysis.randomforest

import ai.vital.aspen.analysis.training.ModelTrainingJob
import java.util.HashMap
import scala.collection.JavaConversions._
import org.apache.spark.mllib.tree.RandomForest

class RandomForestTraining {
  
  /*
  def train(categories:  Array[String], catMap: HashMap[Int, Int]) : Unit = {
    
      val numClasses = categories.length
      val categoricalFeaturesInfo = catMap.toMap
      val numTrees = 20 // Use more in practice.
      val featureSubsetStrategy = "auto" // Let the algorithm choose.
      val impurity = "gini"
      val maxDepth = 20
      val maxBins = 32
      
      val model = RandomForest.trainClassifier(vectorized, numClasses, categoricalFeaturesInfo,
          numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
          
          //not until spark 1.3.0
          //model.save(sc, "myModelPath")
    
    
  }
  */
  
}