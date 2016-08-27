package ai.vital.aspen.analysis.training.impl

import ai.vital.aspen.analysis.training.AbstractModelTrainingTaskImpl
import ai.vital.aspen.groovy.predict.tasks.CalculateAggregationValueTask
import org.apache.spark.SparkContext
import org.apache.spark.Accumulator
import ai.vital.predictmodel.Aggregate.Function
import ai.vital.vitalsigns.VitalSigns
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.aspen.groovy.featureextraction.PredictionModelAnalyzer
import ai.vital.aspen.analysis.training.ModelTrainingJob

class CalculateAggregationValueTaskImpl(sc: SparkContext, task: CalculateAggregationValueTask) extends AbstractModelTrainingTaskImpl[CalculateAggregationValueTask](sc, task) {
    
  def checkDependencies(): Unit = {

    val trainRDD = ModelTrainingJob.getDataset(task.datasetName)
    
  }

  def execute(): Unit = {

    val datasetName = task.datasetName;
      
    val a = task.aggregate
      
    var acc : Accumulator[Int] = null; 
        
    if(a.getFunction == Function.AVERAGE) {
      acc = sc.accumulator(0);
    }
      
    val aspenModel = task.getModel
      
    val trainRDD = ModelTrainingJob.getDataset(datasetName)
          
    val numerics = trainRDD.map { pair =>
                
      val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
                
      val vitalBlock = new VitalBlock(inputObjects)
                
      val aggFunctions = PredictionModelAnalyzer.getAggregationFunctions(aspenModel.getModelConfig, a)
            
      val ex = aspenModel.getFeatureExtraction
                
      val features = ex.extractFeatures(vitalBlock, aggFunctions)
                
      val fv = features.get( a.getRequires().get(0) )
            
      if(fv == null) {
        0d
      } else {
        if( ! fv.isInstanceOf[Number] ) {
          throw new RuntimeException("Expected double value")
        }
                
        if(acc != null) {
          acc += 1
        }
                
        fv.asInstanceOf[Number].doubleValue()
      }
                
    }
      
    var aggV : java.lang.Double = null
        
    if(a.getFunction == Function.AVERAGE) {
          
      if(acc.value == 0) {
            
        aggV = 0d;
            
      } else {
            
        val reduced = numerics.reduce { (a1, a2) =>
          a1 + a2
        }
            
        aggV = reduced /acc.value
            
      } 
          
    } else if(a.getFunction == Function.SUM) {
      aggV = numerics.sum()
    } else if(a.getFunction == Function.MIN) {
      aggV = numerics.min()
    } else if(a.getFunction == Function.MAX) {
      aggV = numerics.max()
    } else {
      throw new RuntimeException("Unhandled aggregation function: " + a.getFunction)
    }

    aspenModel.getAggregationResults.put(a.getProvides, aggV)
    
    task.getParamsMap.put(a.getProvides + CalculateAggregationValueTask.AGGREGATE_VALUE_SUFFIX, aggV);
        
    
  }
}