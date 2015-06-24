package ai.vital.aspen.analysis.pagerank

import ai.vital.aspen.model.AspenPageRankPredictionModel
import ai.vital.aspen.analysis.training.AbstractTraining
import ai.vital.aspen.util.SetOnceHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.Graph
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import ai.vital.vitalsigns.VitalSigns
import scala.collection.JavaConversions._
import ai.vital.vitalsigns.model.VITAL_Edge
import ai.vital.vitalsigns.model.VITAL_Node
import java.util.ArrayList
import org.apache.spark.SparkContext._
import ai.vital.aspen.model.PageRankPrediction
import ai.vital.vitalsigns.block.CompactStringSerializer
import ai.vital.vitalsigns.model.GraphObject
import org.apache.hadoop.io.Text
import ai.vital.hadoop.writable.VitalBytesWritable
import org.apache.hadoop.fs.FileSystem
import ai.vital.aspen.analysis.training.ModelTrainingJob
import org.apache.hadoop.fs.Path
import ai.vital.vitalsigns.block.BlockCompactStringSerializer
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.io.BufferedWriter
import ai.vital.aspen.groovy.predict.tasks.TrainModelTask
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock

class AspenPageRankTraining(model: AspenPageRankPredictionModel, task: TrainModelTask) extends AbstractTraining[AspenPageRankPredictionModel](model) {
  
  def train(globalContext: java.util.Map[String, Object], trainRDD: RDD[(String, Array[Byte])]): java.io.Serializable = {
    
    throw new RuntimeException("Moved to separate tasks!")
    
  }
}