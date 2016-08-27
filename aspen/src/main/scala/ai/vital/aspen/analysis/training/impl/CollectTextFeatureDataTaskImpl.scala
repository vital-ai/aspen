package ai.vital.aspen.analysis.training.impl

import ai.vital.aspen.analysis.training.AbstractModelTrainingTaskImpl
import ai.vital.aspen.groovy.predict.tasks.CollectTextFeatureDataTask
import org.apache.spark.SparkContext
import ai.vital.aspen.analysis.training.ModelTrainingJob
import ai.vital.aspen.groovy.predict.tasks.CountDatasetTask
import org.apache.spark.rdd.RDD
import ai.vital.vitalsigns.VitalSigns
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import java.util.HashSet
import scala.collection.JavaConversions._
import ai.vital.aspen.groovy.featureextraction.Dictionary
import ai.vital.aspen.groovy.featureextraction.TextFeatureData

class CollectTextFeatureDataTaskImpl(sc: SparkContext, task: CollectTextFeatureDataTask) extends AbstractModelTrainingTaskImpl[CollectTextFeatureDataTask](sc, task) {
  def checkDependencies(): Unit = {
    val trainRDD = ModelTrainingJob.getDataset(task.datasetName)
  }

  def execute(): Unit = {

    val trainRDD = ModelTrainingJob.getDataset(task.datasetName)  
    
    val feature = task.feature
    
    val minDF = feature.getMinDF
    
    val totalDocs = task.getParamsMap.get(task.datasetName + CountDatasetTask.DOCS_COUNT_SUFFIX)
    
    val maxDF = ( ( feature.getMaxDFP * totalDocs.asInstanceOf[Int] ) / 100f ).toInt
    
    val aspenModel = task.getModel
    
    val wordsRDD: RDD[String] = trainRDD.flatMap { pair =>

      val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
              
      val vitalBlock = new VitalBlock(inputObjects)
            
      var l = new HashSet[String]()
  
      val ex = aspenModel.getFeatureExtraction
          
      val featuresMap = ex.extractFeatures(vitalBlock)
          
      val textObj = featuresMap.get(feature.getName)
          
      if(textObj != null) {
            
        for (x <- textObj.asInstanceOf[String].toLowerCase().split("\\s+") ) {
          if (x.isEmpty()) {
                
          } else {
            l.add(x)
          }
        }
      }
      
      l.toSeq

    }
        
    val wordsOccurences = wordsRDD.map(x => (x, new Integer(1))).reduceByKey((i1, i2) => i1 + i2).filter(wordc => wordc._2 >= minDF && wordc._2 <= maxDF)

    val dict = Dictionary.createDictionary(mapAsJavaMap( wordsOccurences.collectAsMap() ) , minDF, maxDF) 
    
    val tfd = new TextFeatureData()
    tfd.setDictionary(dict)
        
    aspenModel.getFeaturesData.put(task.feature.getName, tfd)
    task.getParamsMap.put(task.feature.getName + CollectTextFeatureDataTask.TEXT_FEATURE_DATA_SUFFIX, tfd)
    
  }
  
}