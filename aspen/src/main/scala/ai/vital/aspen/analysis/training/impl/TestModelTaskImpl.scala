package ai.vital.aspen.analysis.training.impl

import ai.vital.aspen.analysis.training.AbstractModelTrainingTaskImpl
import ai.vital.aspen.groovy.predict.tasks.TestModelTask
import org.apache.spark.SparkContext
import ai.vital.aspen.analysis.training.ModelTrainingJob
import ai.vital.aspen.model.CollaborativeFilteringPredictionModel
import ai.vital.aspen.model.CollaborativeFilteringPredictionModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.Rating
import ai.vital.aspen.model.DecisionTreePredictionModel
import ai.vital.aspen.model.NaiveBayesPredictionModel
import ai.vital.aspen.model.KMeansPredictionModel
import ai.vital.aspen.model.RandomForestPredictionModel
import ai.vital.aspen.model.RandomForestRegressionModel
import ai.vital.aspen.model.LinearRegressionModel
import ai.vital.aspen.model.SVMWithSGDPredictionModel
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import ai.vital.aspen.model.LogisticRegressionPredictionModel
import ai.vital.aspen.model.DecisionTreeRegressionModel
import ai.vital.aspen.model.GradientBoostedTreesPredictionModel
import ai.vital.aspen.model.GradientBoostedTreesRegressionModel
import ai.vital.aspen.model.IsotonicRegressionModel
import ai.vital.aspen.model.GaussianMixturePredictionModel
import ai.vital.aspen.model.PageRankPredictionModel

class TestModelTaskImpl(sc: SparkContext, task: TestModelTask) extends AbstractModelTrainingTaskImpl[TestModelTask](sc, task) {
  
  def checkDependencies(): Unit = {
     
    val testRDD = ModelTrainingJob.getDataset(task.datasetName)
    
  }

  def execute(): Unit = {
    
    val aspenModel = task.getModel
    
    val mtj = ModelTrainingJob
    
    val testRDD = mtj.getDataset(task.datasetName)
    
    if(CollaborativeFilteringPredictionModel.spark_collaborative_filtering_prediction.equals(aspenModel.getType)) {
          
      val cfpm = aspenModel.asInstanceOf[CollaborativeFilteringPredictionModel]
          
      val values = mtj.globalContext.get("collaborative-filtering-rdd").asInstanceOf[RDD[(String, String, Double)]] 
          
      val usersProducts = values.map( triple => {
        (cfpm.getUserURI2ID().get(triple._1).toInt, cfpm.getProductURI2ID().get(triple._2).toInt )
      }) 
          
      val predictions = cfpm.getModel().predict(usersProducts).map { case Rating(user, product, rate) => 
        ((user, product), rate)
      }
          
      val ratings = mtj.globalContext.get("collaborative-filtering-ratings").asInstanceOf[RDD[Rating]]
      val ratesAndPreds = ratings.map { case Rating(user, product, rate) => 
        ((user, product), rate)
      }.join(predictions)
      val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) => 
      val err = (r1 - r2)
        err * err
      }.mean()
          
      val msg = "Mean Squared Error = " + MSE
          
      println(msg)
      cfpm.setError(msg)
          
    } else if( DecisionTreePredictionModel.spark_decision_tree_prediction.equals(aspenModel.getType)) {
    
      println("Testing ...")
          
      println("Test documents count: " + testRDD.count())
          
      val vectorizedTest = mtj.vectorize(testRDD, aspenModel)
          
      val labelAndPreds = vectorizedTest.map { point =>
      val prediction = aspenModel.asInstanceOf[DecisionTreePredictionModel].getModel().predict(point.features)
        (point.label, prediction)
      }
          
      val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / vectorizedTest.count()
          
      val msg = "Test Error = " + testErr
      println(msg)
      aspenModel.asInstanceOf[DecisionTreePredictionModel].setError(msg)
      
    } else if( DecisionTreeRegressionModel.spark_decision_tree_regression.equals(aspenModel.getType)) {
      
      val dtrm = aspenModel.asInstanceOf[DecisionTreeRegressionModel]
      
      val vectorizedTest = mtj.vectorize(testRDD, aspenModel)
      
      val labelsAndPredictions = vectorizedTest.map { point =>
        val prediction = dtrm.model.predict(point.features)
        (point.label, prediction)
      }
      
      val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
      var msg = "Test Mean Squared Error = " + testMSE
      msg += ( "\nLearned regression tree model:\n" + dtrm.model.toDebugString )
      
      println(msg)
      
      dtrm.setError(msg)
      
    } else if( GaussianMixturePredictionModel.spark_gaussian_mixture_prediction.equals(aspenModel.getType)) {
      
      println("GAUSSIAN MIXTURE does not provide testing implementation")
      
    } else if( GradientBoostedTreesPredictionModel.spark_gradient_boosted_trees_prediction.equals(aspenModel.getType)) {
      
    	val gbtpm = aspenModel.asInstanceOf[GradientBoostedTreesPredictionModel]
      
      val vectorizedTest = mtj.vectorize(testRDD, aspenModel)
      
      // Evaluate model on test instances and compute test error
      val labelAndPreds = vectorizedTest.map { point =>
        val prediction = gbtpm.model.predict(point.features)
        (point.label, prediction)
      }
      
      val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / vectorizedTest.count()
      var msg = ("Test Error = " + testErr)
      msg += ("\nLearned classification GBT model:\n" + gbtpm.model.toDebugString)
      
      println(msg)
      
      gbtpm.setError(msg)
      
    } else if( GradientBoostedTreesRegressionModel.spark_gradient_boosted_trees_regression.equals(aspenModel.getType)) {
      
      val gbtrm = aspenModel.asInstanceOf[GradientBoostedTreesRegressionModel]
      
      val vectorizedTest = mtj.vectorize(testRDD, aspenModel)
      // Evaluate model on test instances and compute test error
      val labelsAndPredictions = vectorizedTest.map { point =>
      val prediction = gbtrm.model.predict(point.features)
        (point.label, prediction)
      }
      val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
      
      var msg = ("Test Mean Squared Error = " + testMSE)
      msg += ("\nLearned regression GBT model:\n" + gbtrm.model.toDebugString)
      
      println(msg)
      
      gbtrm.setError(msg)
      
          } else if(IsotonicRegressionModel.spark_isotonic_regression.equals(aspenModel.getType)) {

      var sirm = aspenModel.asInstanceOf[IsotonicRegressionModel]
      
      val predictionAndLabel = mtj.vectorize(testRDD, sirm).map { point =>
        val predictedLabel = sirm.model.predict(point.features(0))
        (predictedLabel, point.label)
      }

      //   Calculate mean squared error between predicted and real labels.
      val meanSquaredError = predictionAndLabel.map{case(p, l) => math.pow((p - l), 2)}.mean()
      val msg = ("Mean Squared Error = " + meanSquaredError)
      
      println(msg)
      
      sirm.setError(msg)
      
    } else if(LogisticRegressionPredictionModel.spark_logistic_regression_prediction.equals(aspenModel.getType)) {
      
      val lrpm = aspenModel.asInstanceOf[LogisticRegressionPredictionModel]
      
      val vectorizedTest = mtj.vectorize(testRDD, aspenModel)
      
      // Compute raw scores on the test set.
      val predictionAndLabels = vectorizedTest.map { lp =>
        val prediction = lrpm.model.predict(lp.features)
        (prediction, lp.label)
      }
      
      // Get evaluation metrics.
      val metrics = new MulticlassMetrics(predictionAndLabels)
      val precision = metrics.precision
      val msg = "Precision = " + precision
      
      println(msg)
      
      lrpm.setError(msg)
      
    } else if( NaiveBayesPredictionModel.spark_naive_bayes_prediction.equals(aspenModel.getType)) {

      println("Testing ...")
          
      println("Test documents count: " + testRDD.count())
          
      val vectorizedTest = mtj.vectorize(testRDD, aspenModel)
          
      val predictionAndLabel = vectorizedTest.map(p => (aspenModel.asInstanceOf[NaiveBayesPredictionModel].getModel.predict(p.features), p.label))
      val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / vectorizedTest.count()
          
      val msg = "Accuracy: " + accuracy;
          
      println(msg)
          
      aspenModel.asInstanceOf[NaiveBayesPredictionModel].setError(msg);
          
    } else if(KMeansPredictionModel.spark_kmeans_prediction.equals(aspenModel.getType)) {
          
      println("KMEANS does not provide testing implementation")
      
    } else if( LinearRegressionModel.spark_linear_regression.equals(aspenModel.getType)) {
          
      val sprm = aspenModel.asInstanceOf[LinearRegressionModel];
          
      // Evaluate model on training examples and compute training error
      val valuesAndPreds = mtj.vectorize(testRDD, aspenModel).map { point =>
//        println ("Test Point: " + point)
        val scaledPoint = sprm.scaleLabeledPoint(point)
//        println ("Test Point scaled: " + scaledPoint)
        val prediction = sprm.getModel().predict(scaledPoint.features)
//        println("Prediction: " + prediction)
        var original = sprm.scaledBack(prediction);
        println("Input: " + prediction + " rescaled: " + original)
        (point.label, original)
      
      }
          
      val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
      val msg = "training Mean Squared Error = " + MSE
          
      println(msg)
          
      sprm.setError(msg)
      
    } else if( RandomForestPredictionModel.spark_randomforest_prediction.equals(aspenModel.getType ) ) {
          
      println("Testing ...")
          
      println("Test documents count: " + testRDD.count())
          
      val vectorizedTest = mtj.vectorize(testRDD, aspenModel)
          
          
    //    val predictionAndLabel = vectorizedTest.map(p => (model.predict(p.features), p.label))
    //    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / vectorizedTest.count()
    //
    //    println("Accuracy: " + accuracy)
          
          // Evaluate model on test instances and compute test error
      val labelAndPreds = vectorizedTest.map { point =>
      val prediction = aspenModel.asInstanceOf[RandomForestPredictionModel].getModel.predict(point.features)
        (point.label, prediction)
      }
          
      val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / vectorizedTest.count()
    //    println("Learned classification forest model:\n" + model.toDebugString)
              
      val msg = "Test Error = " + testErr
      println(msg)
          
      aspenModel.asInstanceOf[RandomForestPredictionModel].setError(msg)
      
    } else if( PageRankPredictionModel.spark_page_rank_prediction.equals(aspenModel.getType)) {
      
      println("PAGE RANK does not provide testing implementation")
      
    } else if( RandomForestRegressionModel.spark_randomforest_regression.equals(aspenModel.getType)) {
          
      val rfrm = aspenModel.asInstanceOf[RandomForestRegressionModel];
          
      // Evaluate model on training examples and compute training error
      val valuesAndPreds = mtj.vectorize(testRDD, aspenModel).map { point =>
        val prediction = rfrm.getModel().predict(point.features)
        (point.label, prediction)
      }
          
      val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
      var msg = "training Mean Squared Error = " + MSE
      msg = msg + "\nLearned regression forest model:\n" + rfrm.getModel().toDebugString
      println(msg)
          
      rfrm.setError(msg)
      
    } else if( SVMWithSGDPredictionModel.spark_svm_w_sgd_prediction.equals(aspenModel.getType)) {
      
      val swspm = aspenModel.asInstanceOf[SVMWithSGDPredictionModel]
      
      val predictionAndLabels = mtj.vectorize(testRDD, swspm).map { lp =>
      
        val prediction = swspm.model.predict(lp.features)
        
        println(lp.label + " predicted: " + prediction)
        
        (prediction, lp.label)
        
      }
      
      // Get evaluation metrics.
      val metrics = new BinaryClassificationMetrics(predictionAndLabels)
      val auROC = metrics.areaUnderROC()

      val msg = "Area under ROC = " + auROC

      println(msg)
      
      swspm.setError(msg)
      
    } else {
      throw new RuntimeException("Unhandled model testing: " + aspenModel.getType)
    }
    
  }
  
}