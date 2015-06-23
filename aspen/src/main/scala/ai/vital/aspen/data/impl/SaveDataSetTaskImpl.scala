package ai.vital.aspen.data.impl

import ai.vital.aspen.groovy.data.tasks.SaveDataSetTask
import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.job.AbstractJob
import ai.vital.hadoop.writable.VitalBytesWritable
import org.apache.hadoop.io.Text

class SaveDataSetTaskImpl(job: AbstractJob, task: SaveDataSetTask) extends TaskImpl[SaveDataSetTask](job.sparkContext, task) {
  
  def checkDependencies(): Unit = {
  }

  def execute(): Unit = {
    
     val rdd = job.getDataset(task.datasetName);

     val hadoopOutput = rdd.map( pair =>
       (new Text(pair._1), new VitalBytesWritable(pair._2))
     )
          
     hadoopOutput.saveAsSequenceFile(task.outputPath)
     
     task.getParamsMap.put(SaveDataSetTask.SAVE_DATASET_PREFIX + task.datasetName, java.lang.Boolean.TRUE);
    
  }
}