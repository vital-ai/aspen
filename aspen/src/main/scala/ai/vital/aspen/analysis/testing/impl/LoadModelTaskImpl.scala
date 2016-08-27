package ai.vital.aspen.analysis.testing.impl

import ai.vital.aspen.task.TaskImpl
import ai.vital.aspen.groovy.predict.tasks.LoadModelTask
import ai.vital.aspen.job.AbstractJob
import ai.vital.aspen.groovy.AspenGroovyConfig
import ai.vital.aspen.groovy.modelmanager.ModelManager
import org.apache.hadoop.fs.Path

class LoadModelTaskImpl(job: AbstractJob, task: LoadModelTask) extends TaskImpl[LoadModelTask](job.sparkContext, task) {
  def checkDependencies(): Unit = {

  }

  def execute(): Unit = {
    val mt2c = AspenGroovyConfig.get.modelType2Class
    mt2c.putAll(job.getModelManagerMap())
    
    val modelManager = new ModelManager()
    
    println("Loading model ...")
    
    val modelPath = new Path(task.modelPath)
    
    val aspenModel = modelManager.loadModel(modelPath.toUri().toString())
    
    println("Model loaded successfully")
    
    job.unloadDynamicDomains()
    
    job.loadDynamicDomainJars(aspenModel)
    
    task.getParamsMap.put(LoadModelTask.LOADED_MODEL_PREFIX + task.modelPath, aspenModel)
    
  }
}