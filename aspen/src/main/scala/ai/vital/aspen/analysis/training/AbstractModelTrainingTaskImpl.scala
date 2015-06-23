package ai.vital.aspen.analysis.training

import ai.vital.aspen.model.PredictionModel
import ai.vital.aspen.groovy.predict.ModelTrainingTask
import org.apache.spark.SparkContext
import ai.vital.aspen.task.TaskImpl

abstract class AbstractModelTrainingTaskImpl[T <: ModelTrainingTask](sc: SparkContext, task: T) extends TaskImpl(sc, task) {

  def checkDependencies() : Unit
 
  def execute() : Unit
  
}