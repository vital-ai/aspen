package ai.vital.aspen.job

import ai.vital.aspen.groovy.task.AbstractTask
import scala.collection.JavaConversions._
import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.groovy.predict.tasks.CalculateAggregationValueTask
import ai.vital.aspen.analysis.training.impl.CalculateAggregationValueTaskImpl
import ai.vital.aspen.groovy.predict.tasks.CollectCategoricalFeatureTaxonomyDataTask
import ai.vital.aspen.analysis.training.impl.CollectCategoricalFeatureTaxonomyDataTaskImpl
import ai.vital.aspen.groovy.predict.tasks.CollectNumericalFeatureDataTask
import ai.vital.aspen.analysis.training.impl.CollectNumericalFeatureDataTaskImpl
import ai.vital.aspen.groovy.predict.tasks.CollectTextFeatureDataTask
import ai.vital.aspen.analysis.training.impl.CollectTextFeatureDataTaskImpl
import ai.vital.aspen.groovy.predict.tasks.CollectTrainTaxonomyDataTask
import ai.vital.aspen.analysis.training.impl.CollectTrainTaxonomyDataTaskImpl
import ai.vital.aspen.groovy.predict.tasks.CountDatasetTask
import ai.vital.aspen.analysis.training.impl.CountDatasetTaskImpl
import ai.vital.aspen.groovy.predict.tasks.FeatureQueryTask
import ai.vital.aspen.groovy.data.tasks.LoadDataSetTask
import ai.vital.aspen.groovy.predict.tasks.SaveModelTask
import ai.vital.aspen.data.impl.LoadDataSetTaskImpl
import ai.vital.aspen.groovy.data.tasks.SplitDatasetTask
import ai.vital.aspen.analysis.training.impl.SplitDatasetTaskImpl
import ai.vital.aspen.groovy.predict.tasks.TrainModelTask
import ai.vital.aspen.groovy.predict.tasks.TestModelTask
import ai.vital.aspen.analysis.training.impl.TrainModelTaskImpl
import ai.vital.aspen.analysis.training.impl.TestModelTaskImpl
import ai.vital.aspen.analysis.training.impl.SaveModelTaskImpl
import ai.vital.aspen.data.impl.FeatureQueryTaskImpl
import ai.vital.aspen.groovy.convert.tasks.CheckPathTask
import ai.vital.aspen.convert.impl.CheckPathTaskImpl
import ai.vital.aspen.groovy.convert.tasks.ConvertBlockToSequenceTask
import ai.vital.aspen.convert.impl.ConvertBlockToSequenceTaskImpl
import ai.vital.aspen.groovy.convert.tasks.DeletePathTask
import ai.vital.aspen.convert.impl.DeletePathTaskImpl
import ai.vital.aspen.groovy.convert.tasks.ConvertSequenceToBlockTask
import ai.vital.aspen.convert.impl.ConvertSequenceToBlockTaskImpl
import ai.vital.aspen.groovy.data.tasks.ResolveURIReferencesTask
import ai.vital.aspen.data.impl.ResolveURIsTaskImpl
import ai.vital.aspen.groovy.predict.tasks.LoadModelTask
import ai.vital.aspen.analysis.testing.impl.LoadModelTaskImpl
import ai.vital.aspen.groovy.predict.tasks.TestModelIndependentTask
import ai.vital.aspen.analysis.testing.impl.TestModelIndependentTaskImpl
import ai.vital.aspen.groovy.predict.tasks.ModelPredictTask
import ai.vital.aspen.analysis.predict.impl.ModelPredictTaskImpl
import ai.vital.aspen.groovy.data.tasks.SaveDataSetTask
import ai.vital.aspen.data.impl.SaveDataSetTaskImpl
import ai.vital.aspen.groovy.select.tasks.SelectDataSetTask
import ai.vital.aspen.select.impl.SelectDatasetTaskImpl
import ai.vital.aspen.groovy.data.tasks.FilterDatasetTask
import ai.vital.aspen.data.impl.FilterDatasetTaskImpl
import ai.vital.aspen.groovy.predict.tasks.CalculatePageRankValuesTask
import ai.vital.aspen.analysis.pagerank.CalculatePageRankValuesTaskImpl
import ai.vital.aspen.groovy.predict.tasks.AssignPageRankValuesTask
import ai.vital.aspen.analysis.pagerank.AssignPageRankValuesTaskImpl
import ai.vital.aspen.data.DowngradeUpgradeProcedureTask
import ai.vital.aspen.data.impl.DowngradeUpgradeProcedureTaskImpl

class TasksHandler {

  def handleTasksList(job: AbstractJob, tasks: java.util.List[AbstractTask]) : Unit = {
    
        val totalTasks = tasks.size()
    
    var currentTask = 0
    
    for( task <- tasks ) {
    
      
      currentTask = currentTask + 1
      
      println ( "Executing task: " + task.getClass.getCanonicalName + " [" + currentTask + " of " + totalTasks + "]")
      
      for(i <- task.getRequiredParams) {
        if(!task.getParamsMap.containsKey(i)) throw new RuntimeException("Task " + task.getClass.getSimpleName + " input param not set: " + i)
      }
      
      //any inner dependencies
      task.checkDepenedencies()
      
      val sc = job.sparkContext
      
      var taskImpl : TaskImpl[_] = null
      
        
      if(task.isInstanceOf[AssignPageRankValuesTask]) {
        
        taskImpl = new AssignPageRankValuesTaskImpl(job, task.asInstanceOf[AssignPageRankValuesTask])
        
      } else if(task.isInstanceOf[CalculateAggregationValueTask]) {
        
        taskImpl = new CalculateAggregationValueTaskImpl(sc, task.asInstanceOf[CalculateAggregationValueTask])
        
      } else if(task.isInstanceOf[CalculatePageRankValuesTask]) {
        
        taskImpl = new CalculatePageRankValuesTaskImpl(job, task.asInstanceOf[CalculatePageRankValuesTask])
        
      } else if(task.isInstanceOf[CheckPathTask]) {
        
        taskImpl = new CheckPathTaskImpl(job, task.asInstanceOf[CheckPathTask])
        
      } else if(task.isInstanceOf[CollectCategoricalFeatureTaxonomyDataTask]) {
        
        taskImpl = new CollectCategoricalFeatureTaxonomyDataTaskImpl(sc, task.asInstanceOf[CollectCategoricalFeatureTaxonomyDataTask])
        
      } else if(task.isInstanceOf[CollectNumericalFeatureDataTask]) {

        taskImpl = new CollectNumericalFeatureDataTaskImpl(sc, task.asInstanceOf[CollectNumericalFeatureDataTask])
        
      } else if(task.isInstanceOf[CollectTextFeatureDataTask]) {
        
        taskImpl = new CollectTextFeatureDataTaskImpl(sc, task.asInstanceOf[CollectTextFeatureDataTask])
        
      } else if(task.isInstanceOf[CollectTrainTaxonomyDataTask]) {
        
        taskImpl = new CollectTrainTaxonomyDataTaskImpl(sc, task.asInstanceOf[CollectTrainTaxonomyDataTask])
        
      } else if(task.isInstanceOf[ConvertBlockToSequenceTask]) {
        
        taskImpl = new ConvertBlockToSequenceTaskImpl(job, task.asInstanceOf[ConvertBlockToSequenceTask])
        
      } else if(task.isInstanceOf[ConvertSequenceToBlockTask]) {
        
        taskImpl = new ConvertSequenceToBlockTaskImpl(job, task.asInstanceOf[ConvertSequenceToBlockTask])
        
      } else if(task.isInstanceOf[CountDatasetTask]) {
        
        taskImpl = new CountDatasetTaskImpl(sc, task.asInstanceOf[CountDatasetTask])

      } else if(task.isInstanceOf[DeletePathTask]) {
        
    	  taskImpl = new DeletePathTaskImpl(job, task.asInstanceOf[DeletePathTask])
        
      } else if(task.isInstanceOf[DowngradeUpgradeProcedureTask]) {
        
        taskImpl = new DowngradeUpgradeProcedureTaskImpl(job, task.asInstanceOf[DowngradeUpgradeProcedureTask])
        
      } else if(task.isInstanceOf[FeatureQueryTask]) {
        
        taskImpl = new FeatureQueryTaskImpl(job, task.asInstanceOf[FeatureQueryTask])
        
      } else if(task.isInstanceOf[FilterDatasetTask]) {
        
        taskImpl = new FilterDatasetTaskImpl(job, task.asInstanceOf[FilterDatasetTask])
        
      } else if(task.isInstanceOf[LoadDataSetTask]) {
        
        taskImpl = new LoadDataSetTaskImpl(job, task.asInstanceOf[LoadDataSetTask])
        
      } else if(task.isInstanceOf[LoadModelTask]) {
        
        taskImpl = new LoadModelTaskImpl(job, task.asInstanceOf[LoadModelTask])
        
      } else if(task.isInstanceOf[ModelPredictTask]) {
        
        taskImpl = new ModelPredictTaskImpl(job, task.asInstanceOf[ModelPredictTask])
        
      } else if(task.isInstanceOf[ResolveURIReferencesTask]) {
        
        taskImpl = new ResolveURIsTaskImpl(job, task.asInstanceOf[ResolveURIReferencesTask])
      
      } else if(task.isInstanceOf[SaveDataSetTask]) {
        
        taskImpl = new SaveDataSetTaskImpl(job, task.asInstanceOf[SaveDataSetTask])
        
      } else if(task.isInstanceOf[SaveModelTask]) {
        
        val smt = task.asInstanceOf[SaveModelTask]
        
        //model packaging is now implemented in the model itsefl
    
        taskImpl = new SaveModelTaskImpl(job, task.asInstanceOf[SaveModelTask])
        
      } else if(task.isInstanceOf[SelectDataSetTask]) {
        
        taskImpl = new SelectDatasetTaskImpl(job, task.asInstanceOf[SelectDataSetTask])
        
      } else if(task.isInstanceOf[SplitDatasetTask]) {
        
        taskImpl = new SplitDatasetTaskImpl(job, task.asInstanceOf[SplitDatasetTask])
        
      } else if(task.isInstanceOf[TrainModelTask]) {

        taskImpl = new TrainModelTaskImpl(sc, task.asInstanceOf[TrainModelTask])

      } else if(task.isInstanceOf[TestModelIndependentTask]) {
        
        taskImpl = new TestModelIndependentTaskImpl(job, task.asInstanceOf[TestModelIndependentTask])
        
      } else if(task.isInstanceOf[TestModelTask]) {

        taskImpl = new TestModelTaskImpl(sc, task.asInstanceOf[TestModelTask])
        
      } else {
        throw new RuntimeException("Unhandled task: " + task.getClass.getCanonicalName);
      }
      
      
      if(taskImpl != null) {
        
        taskImpl.checkDependencies()
        
        taskImpl.execute()
        
      }

      for(x <- task.getOutputParams) {
        if(!task.getParamsMap.containsKey(x)) throw new RuntimeException("Task " + task.getClass.getCanonicalName + " did not return param: " + x);
      }
      
      
      //inner validation
      task.onTaskComplete()
      
      
    }
    
  }
  
}