package ai.vital.aspen.analysis.svm

import ai.vital.aspen.analysis.training.AbstractTraining
import ai.vital.aspen.model.SVMWithSGDPredictionModel
import ai.vital.aspen.util.SetOnceHashMap
import org.apache.spark.rdd.RDD
import ai.vital.aspen.analysis.training.ModelTrainingJob
import org.apache.spark.mllib.classification.SVMWithSGD

class SVMWithSGDTraining(model: SVMWithSGDPredictionModel) extends AbstractTraining[SVMWithSGDPredictionModel](model) { {
  
}
  def train(globalContext: SetOnceHashMap, trainRDD: RDD[(String, Array[Byte])]): java.io.Serializable = {

    val vectorized = ModelTrainingJob.vectorize(trainRDD, model);
    
    // Run training algorithm to build the model
    val numIterations = model.numIterations
    val trained = SVMWithSGD.train(vectorized, numIterations)

    // Clear the default threshold.
    trained.clearThreshold()
    
    model.model = trained
    
    return trained

    
    
//// Compute raw scores on the test set.
//val scoreAndLabels = test.map { point =>
//  val score = model.predict(point.features)
//  (score, point.label)
//}
//
//// Get evaluation metrics.
//val metrics = new BinaryClassificationMetrics(scoreAndLabels)
//val auROC = metrics.areaUnderROC()
//
//println("Area under ROC = " + auROC)
//
//// Save and load model
//model.save(sc, "myModelPath")
//val sameModel = SVMModel.load(sc, "myModelPath")
    
  }
}