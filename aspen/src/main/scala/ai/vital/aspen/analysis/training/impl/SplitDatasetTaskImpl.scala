package ai.vital.aspen.analysis.training.impl

import ai.vital.aspen.groovy.data.tasks.SplitDatasetTask
import org.apache.spark.SparkContext
import ai.vital.aspen.analysis.training.ModelTrainingJob
import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.job.AbstractJob

class SplitDatasetTaskImpl(job: AbstractJob, task: SplitDatasetTask) extends TaskImpl[SplitDatasetTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
    
    ModelTrainingJob.getDataset(task.inputDatasetName)
    
  }

  def execute(): Unit = {

    val mtj = job
    val inputRDD = mtj.getDataset(task.inputDatasetName)
      
    val splits = inputRDD.randomSplit(Array(task.firstSplitRatio, 1 - task.firstSplitRatio), seed = 11L)
      
    if(mtj.isNamedRDDSupported()) {
        
      mtj.namedRdds.update(task.outputDatasetName1, splits(0))
      mtj.namedRdds.update(task.outputDatasetName2, splits(1))
        
    } else {
        
      mtj.datasetsMap.put(task.outputDatasetName1, splits(0))
      mtj.datasetsMap.put(task.outputDatasetName2, splits(1))
        
    }
      
    task.getParamsMap.put(task.outputDatasetName1, splits(0))
    task.getParamsMap.put(task.outputDatasetName2, splits(1))

  }
  
}