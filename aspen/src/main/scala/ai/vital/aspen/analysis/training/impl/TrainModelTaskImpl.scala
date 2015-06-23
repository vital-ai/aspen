package ai.vital.aspen.analysis.training.impl

import ai.vital.aspen.analysis.training.AbstractModelTrainingTaskImpl
import ai.vital.aspen.groovy.predict.tasks.TrainModelTask
import org.apache.spark.SparkContext
import ai.vital.aspen.analysis.training.ModelTrainingJob
import ai.vital.aspen.analysis.training.AbstractTraining
import ai.vital.aspen.model.AspenCollaborativeFilteringPredictionModel
import ai.vital.aspen.analysis.collaborativefiltering.AspenCollaborativeFilteringTraining
import ai.vital.aspen.model.AspenDecisionTreePredictionModel
import ai.vital.aspen.analysis.decisiontree.AspenDecisionTreeTraining
import ai.vital.aspen.analysis.kmeans.AspenKMeansClusteringTraining
import ai.vital.aspen.model.AspenNaiveBayesPredictionModel
import ai.vital.aspen.model.AspenRandomForestPredictionModel
import ai.vital.aspen.model.AspenKMeansPredictionModel
import ai.vital.aspen.analysis.logisticregression.AspenLogisticRegressionPredictionTraining
import ai.vital.aspen.analysis.naivebayes.AspenNaiveBayesTraining
import ai.vital.aspen.analysis.randomforest.AspenRandomForestTraining
import ai.vital.aspen.analysis.randomforest.AspenRandomForestRegressionTraining
import ai.vital.aspen.model.AspenRandomForestRegressionModel
import ai.vital.aspen.analysis.regression.AspenLinearRegressionTraining
import ai.vital.aspen.model.AspenLinearRegressionModel
import ai.vital.aspen.model.AspenSVMWithSGDPredictionModel
import ai.vital.aspen.analysis.svm.AspenSVMWithSGDTraining
import ai.vital.aspen.model.AspenLogisticRegressionPredictionModel
import ai.vital.aspen.model.AspenDecisionTreeRegressionModel
import ai.vital.aspen.analysis.decisiontree.AspenDecisionTreeRegressionTraining
import ai.vital.aspen.model.AspenGradientBoostedTreesPredictionModel
import ai.vital.aspen.analysis.gradientboostedtrees.AspenGradientBoostedTreesPredictionTraining
import ai.vital.aspen.analysis.gradientboostedtrees.AspenGradientBoostedTreesRegressionTraining
import ai.vital.aspen.model.AspenGradientBoostedTreesRegressionModel
import ai.vital.aspen.model.AspenIsotonicRegressionModel
import ai.vital.aspen.analysis.regression.AspenIsotonicRegressionTraining
import ai.vital.aspen.model.AspenGaussianMixturePredictionModel
import ai.vital.aspen.analysis.gaussianmixture.AspenGaussianMixtureTraining
import ai.vital.aspen.model.AspenPageRankPredictionModel
import ai.vital.aspen.analysis.pagerank.AspenPageRankTraining
import ai.vital.aspen.groovy.predict.tasks.LoadModelTask

class TrainModelTaskImpl(sc: SparkContext, task: TrainModelTask) extends AbstractModelTrainingTaskImpl[TrainModelTask](sc, task) {
  
  def checkDependencies(): Unit = {

    val trainRDD = ModelTrainingJob.getDataset(task.datasetName)

  }

  def execute(): Unit = {

    val trainRDD = ModelTrainingJob.getDataset(task.datasetName)
      
    var trainingImpl : AbstractTraining[_] = null
      
    val aspenModel = task.getModel
      
      if(aspenModel.isInstanceOf[AspenCollaborativeFilteringPredictionModel]) {
        trainingImpl = new AspenCollaborativeFilteringTraining(aspenModel.asInstanceOf[AspenCollaborativeFilteringPredictionModel])
      } else if(aspenModel.isInstanceOf[AspenDecisionTreePredictionModel]){
        trainingImpl = new AspenDecisionTreeTraining(aspenModel.asInstanceOf[AspenDecisionTreePredictionModel])
      } else if(aspenModel.isInstanceOf[AspenDecisionTreeRegressionModel]) {
        trainingImpl = new AspenDecisionTreeRegressionTraining(aspenModel.asInstanceOf[AspenDecisionTreeRegressionModel])
      } else if(aspenModel.isInstanceOf[AspenGaussianMixturePredictionModel]) {
        trainingImpl = new AspenGaussianMixtureTraining(aspenModel.asInstanceOf[AspenGaussianMixturePredictionModel])
      } else if(aspenModel.isInstanceOf[AspenGradientBoostedTreesPredictionModel]) {
        trainingImpl = new AspenGradientBoostedTreesPredictionTraining(aspenModel.asInstanceOf[AspenGradientBoostedTreesPredictionModel])
      } else if(aspenModel.isInstanceOf[AspenGradientBoostedTreesRegressionModel]) {
        trainingImpl = new AspenGradientBoostedTreesRegressionTraining(aspenModel.asInstanceOf[AspenGradientBoostedTreesRegressionModel])
      } else if(aspenModel.isInstanceOf[AspenIsotonicRegressionModel]) {
    	  trainingImpl = new AspenIsotonicRegressionTraining(aspenModel.asInstanceOf[AspenIsotonicRegressionModel])        
      } else if(aspenModel.isInstanceOf[AspenKMeansPredictionModel]) {
    	  trainingImpl = new AspenKMeansClusteringTraining(aspenModel.asInstanceOf[AspenKMeansPredictionModel])
      } else if(aspenModel.isInstanceOf[AspenLinearRegressionModel]) {
    	  trainingImpl = new AspenLinearRegressionTraining(aspenModel.asInstanceOf[AspenLinearRegressionModel])
      } else if(aspenModel.isInstanceOf[AspenLogisticRegressionPredictionModel]) {
        trainingImpl = new AspenLogisticRegressionPredictionTraining(aspenModel.asInstanceOf[AspenLogisticRegressionPredictionModel])
      } else if(aspenModel.isInstanceOf[AspenNaiveBayesPredictionModel]) {
        trainingImpl = new AspenNaiveBayesTraining(aspenModel.asInstanceOf[AspenNaiveBayesPredictionModel])
      } else if(aspenModel.isInstanceOf[AspenPageRankPredictionModel]) {
        trainingImpl = new AspenPageRankTraining(aspenModel.asInstanceOf[AspenPageRankPredictionModel])
      } else if(aspenModel.isInstanceOf[AspenRandomForestPredictionModel]) {
        trainingImpl = new AspenRandomForestTraining(aspenModel.asInstanceOf[AspenRandomForestPredictionModel])
      } else if(aspenModel.isInstanceOf[AspenRandomForestRegressionModel]) {
        trainingImpl = new AspenRandomForestRegressionTraining(aspenModel.asInstanceOf[AspenRandomForestRegressionModel])
      } else if(aspenModel.isInstanceOf[AspenSVMWithSGDPredictionModel]) {
        trainingImpl = new AspenSVMWithSGDTraining(aspenModel.asInstanceOf[AspenSVMWithSGDPredictionModel])
      } else {
        throw new RuntimeException("Unhandled aspen model type: " + aspenModel.getType)
      }
  
      
      var outputModel = trainingImpl.train(task.getParamsMap, trainRDD);

      task.getParamsMap.put(TrainModelTask.MODEL_BINARY, outputModel.asInstanceOf[Object])
      task.getParamsMap.put(LoadModelTask.LOADED_MODEL_PREFIX + task.modelPath, aspenModel)
    
  }
  
}