package ai.vital.aspen.analysis.training.impl

import ai.vital.aspen.analysis.training.AbstractModelTrainingTaskImpl
import ai.vital.aspen.groovy.predict.tasks.SplitDatasetTask
import org.apache.spark.SparkContext
import ai.vital.aspen.analysis.training.ModelTrainingJob

class SplitDatasetTaskImpl(sc: SparkContext, task: SplitDatasetTask) extends AbstractModelTrainingTaskImpl[SplitDatasetTask](sc, task) {
  
  def checkDependencies(): Unit = {
    
    ModelTrainingJob.getDataset(task.inputDatasetName)
    
  }

  def execute(): Unit = {

    val mtj = ModelTrainingJob
    val inputRDD = mtj.getDataset(task.inputDatasetName)
      
    val splits = inputRDD.randomSplit(Array(task.firstSplitRatio, 1 - task.firstSplitRatio), seed = 11L)
      
    if(mtj.isNamedRDDSupported()) {
        
      mtj.namedRdds.update(task.outputDatasetName1, splits(0))
      mtj.namedRdds.update(task.outputDatasetName2, splits(1))
        
    } else {
        
      mtj.datasetsMap.put(task.outputDatasetName1, splits(0))
      mtj.datasetsMap.put(task.outputDatasetName2, splits(1))
        
    }
      
    mtj.globalContext.put(task.outputDatasetName1, splits(0))
    mtj.globalContext.put(task.outputDatasetName2, splits(1))

  }
  
}