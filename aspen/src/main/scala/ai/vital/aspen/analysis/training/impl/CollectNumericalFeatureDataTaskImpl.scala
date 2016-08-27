package ai.vital.aspen.analysis.training.impl

import ai.vital.aspen.analysis.training.AbstractModelTrainingTaskImpl
import ai.vital.aspen.groovy.predict.tasks.CollectNumericalFeatureDataTask
import org.apache.spark.SparkContext
import ai.vital.aspen.groovy.featureextraction.NumericalFeatureData
import ai.vital.aspen.analysis.training.ModelTrainingJob

class CollectNumericalFeatureDataTaskImpl(sc: SparkContext, task: CollectNumericalFeatureDataTask) extends AbstractModelTrainingTaskImpl[CollectNumericalFeatureDataTask](sc, task) {
  
  def checkDependencies(): Unit = {

  }

  def execute(): Unit = {
    
    if( task.getModel.getFeaturesData.get(task.feature.getName) == null ) {
      
      task.getModel.getFeaturesData.put(task.feature.getName, new NumericalFeatureData())
      
    }
    
    task.getParamsMap.put(task.feature.getName + CollectNumericalFeatureDataTask.NUMERICAL_FEATURE_DATA_SUFFIX, task.getModel.getFeaturesData.get(task.feature.getName))
    
  }
  
}