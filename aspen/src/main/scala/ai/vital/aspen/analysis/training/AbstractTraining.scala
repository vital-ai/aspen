package ai.vital.aspen.analysis.training

import scala.collection.JavaConversions._
import ai.vital.aspen.model.PredictionModel
import org.apache.spark.rdd.RDD
import ai.vital.aspen.util.SetOnceHashMap
import java.io.Serializable

/**
 * encapsulates algorithm specific model training phase 
 */
abstract class AbstractTraining[T <: PredictionModel] (model : T) {

  /**
   * Main training block, the implementation should set the model binary serializable object and return it 
   */
  def train(globalContext : SetOnceHashMap, trainRDD : RDD[(String, Array[Byte])]) : Serializable;
  
}