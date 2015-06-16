package ai.vital.aspen.analysis.training.impl

import ai.vital.aspen.analysis.training.AbstractModelTrainingTaskImpl
import ai.vital.aspen.groovy.predict.tasks.TrainModelTask
import org.apache.spark.SparkContext
import ai.vital.aspen.analysis.training.ModelTrainingJob
import ai.vital.aspen.analysis.training.AbstractTraining
import ai.vital.aspen.model.CollaborativeFilteringPredictionModel
import ai.vital.aspen.analysis.collaborativefiltering.CollaborativeFilteringTraining
import ai.vital.aspen.model.DecisionTreePredictionModel
import ai.vital.aspen.analysis.decisiontree.DecisionTreeTraining
import ai.vital.aspen.analysis.kmeans.KMeansClustering
import ai.vital.aspen.model.NaiveBayesPredictionModel
import ai.vital.aspen.model.RandomForestPredictionModel
import ai.vital.aspen.model.KMeansPredictionModel
import ai.vital.aspen.analysis.logisticregression.LogisticRegressionPredictionTraining
import ai.vital.aspen.analysis.naivebayes.NaiveBayesTraining
import ai.vital.aspen.analysis.randomforest.RandomForestTraining
import ai.vital.aspen.analysis.randomforest.RandomForestRegressionTraining
import ai.vital.aspen.model.RandomForestRegressionModel
import ai.vital.aspen.analysis.regression.SparkLinearRegressionTraining
import ai.vital.aspen.model.SparkLinearRegressionModel
import ai.vital.aspen.model.SVMWithSGDPredictionModel
import ai.vital.aspen.analysis.svm.SVMWithSGDTraining
import ai.vital.aspen.model.LogisticRegressionPredictionModel
import ai.vital.aspen.model.DecisionTreeRegressionModel
import ai.vital.aspen.analysis.decisiontree.DecisionTreeRegressionTraining
import ai.vital.aspen.model.GradientBoostedTreesPredictionModel
import ai.vital.aspen.analysis.gradientboostedtrees.GradientBoostedTreesPredictionTraining
import ai.vital.aspen.analysis.gradientboostedtrees.GradientBoostedTreesRegressionTraining
import ai.vital.aspen.model.GradientBoostedTreesRegressionModel
import ai.vital.aspen.model.SparkIsotonicRegressionModel
import ai.vital.aspen.analysis.regression.SparkIsotonicRegressionTraining
import ai.vital.aspen.model.GaussianMixturePredictionModel
import ai.vital.aspen.analysis.gaussianmixture.GaussianMixtureTraining

class TrainModelTaskImpl(sc: SparkContext, task: TrainModelTask) extends AbstractModelTrainingTaskImpl[TrainModelTask](sc, task) {
  
  def checkDependencies(): Unit = {

    val trainRDD = ModelTrainingJob.getDataset(task.datasetName)

  }

  def execute(): Unit = {

    val trainRDD = ModelTrainingJob.getDataset(task.datasetName)
      
    var trainingImpl : AbstractTraining[_] = null
      
    val aspenModel = task.getModel
      
      if(aspenModel.isInstanceOf[CollaborativeFilteringPredictionModel]) {
        trainingImpl = new CollaborativeFilteringTraining(aspenModel.asInstanceOf[CollaborativeFilteringPredictionModel])
      } else if(aspenModel.isInstanceOf[DecisionTreePredictionModel]){
        trainingImpl = new DecisionTreeTraining(aspenModel.asInstanceOf[DecisionTreePredictionModel])
      } else if(aspenModel.isInstanceOf[DecisionTreeRegressionModel]) {
        trainingImpl = new DecisionTreeRegressionTraining(aspenModel.asInstanceOf[DecisionTreeRegressionModel])
      } else if(aspenModel.isInstanceOf[GaussianMixturePredictionModel]) {
        trainingImpl = new GaussianMixtureTraining(aspenModel.asInstanceOf[GaussianMixturePredictionModel])
      } else if(aspenModel.isInstanceOf[GradientBoostedTreesPredictionModel]) {
        trainingImpl = new GradientBoostedTreesPredictionTraining(aspenModel.asInstanceOf[GradientBoostedTreesPredictionModel])
      } else if(aspenModel.isInstanceOf[GradientBoostedTreesRegressionModel]) {
        trainingImpl = new GradientBoostedTreesRegressionTraining(aspenModel.asInstanceOf[GradientBoostedTreesRegressionModel])
      } else if(aspenModel.isInstanceOf[KMeansPredictionModel]) {
        trainingImpl = new KMeansClustering(aspenModel.asInstanceOf[KMeansPredictionModel])
      } else if(aspenModel.isInstanceOf[LogisticRegressionPredictionModel]) {
        trainingImpl = new LogisticRegressionPredictionTraining(aspenModel.asInstanceOf[LogisticRegressionPredictionModel])
      } else if(aspenModel.isInstanceOf[NaiveBayesPredictionModel]) {
        trainingImpl = new NaiveBayesTraining(aspenModel.asInstanceOf[NaiveBayesPredictionModel])
      } else if(aspenModel.isInstanceOf[RandomForestPredictionModel]) {
        trainingImpl = new RandomForestTraining(aspenModel.asInstanceOf[RandomForestPredictionModel])
      } else if(aspenModel.isInstanceOf[RandomForestRegressionModel]) {
        trainingImpl = new RandomForestRegressionTraining(aspenModel.asInstanceOf[RandomForestRegressionModel])
      } else if(aspenModel.isInstanceOf[SparkIsotonicRegressionModel]) {
        trainingImpl = new SparkIsotonicRegressionTraining(aspenModel.asInstanceOf[SparkIsotonicRegressionModel])        
      } else if(aspenModel.isInstanceOf[SparkLinearRegressionModel]) {
        trainingImpl = new SparkLinearRegressionTraining(aspenModel.asInstanceOf[SparkLinearRegressionModel])
      } else if(aspenModel.isInstanceOf[SVMWithSGDPredictionModel]) {
        trainingImpl = new SVMWithSGDTraining(aspenModel.asInstanceOf[SVMWithSGDPredictionModel])
      } else {
        throw new RuntimeException("Unhandled aspen model type: " + aspenModel.getType)
      }
  
      
      var outputModel = trainingImpl.train(ModelTrainingJob.globalContext, trainRDD);

      ModelTrainingJob.globalContext.put(TrainModelTask.MODEL_BINARY, outputModel)
    
  }
  
}