package ai.vital.aspen.analysis.training

import ai.vital.aspen.model.PredictionModel
import ai.vital.aspen.groovy.predict.ModelTrainingTask
import org.apache.spark.SparkContext

abstract class AbstractModelTrainingTaskImpl[T <: ModelTrainingTask] (sc : SparkContext, task: T) {

  def checkDependencies() : Unit
 
  def execute() : Unit
  
}