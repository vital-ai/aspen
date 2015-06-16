package ai.vital.aspen.analysis.training.impl

import ai.vital.aspen.analysis.training.AbstractModelTrainingTaskImpl
import ai.vital.aspen.groovy.predict.tasks.CountDatasetTask
import org.apache.spark.SparkContext
import ai.vital.aspen.analysis.training.ModelTrainingJob

class CountDatasetTaskImpl(sc: SparkContext, task: CountDatasetTask) extends AbstractModelTrainingTaskImpl[CountDatasetTask](sc, task) {
  
  def checkDependencies(): Unit = {
    val trainRDD = ModelTrainingJob.getDataset(task.datasetName)
  }

  def execute(): Unit = {
    
    val trainRDD = ModelTrainingJob.getDataset(task.datasetName)
    val x = trainRDD.count().intValue();
    
    ModelTrainingJob.globalContext.put(task.datasetName + CountDatasetTask.DOCS_COUNT_SUFFIX, x)
    
  }
}