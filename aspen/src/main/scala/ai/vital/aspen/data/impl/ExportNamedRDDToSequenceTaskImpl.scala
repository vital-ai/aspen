package ai.vital.aspen.data.impl

import ai.vital.aspen.data.ExportNamedRDDToSequenceTask
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.data.ExportNamedRDDToSequenceTask
import ai.vital.hadoop.writable.VitalBytesWritable
import org.apache.hadoop.io.Text

class ExportNamedRDDToSequenceTaskImpl(job: AbstractJob, task: ExportNamedRDDToSequenceTask) extends TaskImpl[ExportNamedRDDToSequenceTask](job.sparkContext, task)  {
  
  def checkDependencies(): Unit = {
    
    if( ! job.isNamedRDDSupported() ) {
      throw new RuntimeException("NamedRDDs not supported")
    }
    
  }

  def execute(): Unit = {

    val inputBlockRDD = job.namedRdds.get[(String, Array[Byte])](task.getInputName()).get
    
    val outputPath = task.getOutputPath()
    
    val hadoopOutput = inputBlockRDD.map( pair =>
        (new Text(pair._1), new VitalBytesWritable(pair._2))
      )
          
      hadoopOutput.saveAsSequenceFile(outputPath)
    
  }
  
  
}