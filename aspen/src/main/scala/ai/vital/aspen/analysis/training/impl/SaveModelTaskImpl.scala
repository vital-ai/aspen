package ai.vital.aspen.analysis.training.impl

import ai.vital.aspen.analysis.training.AbstractModelTrainingTaskImpl
import ai.vital.aspen.groovy.predict.tasks.SaveModelTask
import ai.vital.aspen.job.AbstractJob
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

class SaveModelTaskImpl(job: AbstractJob, task: SaveModelTask) extends AbstractModelTrainingTaskImpl[SaveModelTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {

  }

  def execute(): Unit = {
    
    val outputModelPath = new Path(task.modelPath);
    
    val modelFS = FileSystem.get(outputModelPath.toUri(), job.hadoopConfiguration)
//    
    
    val asJar = task.modelPath.endsWith(".jar") || task.modelPath.endsWith(".zip");
    
    task.getModel.persist(modelFS, outputModelPath, asJar)
    
//     if(outputContainerPath != null) {
//       task.getModel.persist(modelFS, outputContainerPath, zipContainer || jarContainer)
//      } else {
//        task.getModel.persist(modelFS, outputModelPath, zipContainer || jarContainer)
//      }
  }
  
  
}