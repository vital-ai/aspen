package ai.vital.aspen.analysis.training.impl

import ai.vital.aspen.analysis.training.AbstractModelTrainingTaskImpl
import ai.vital.aspen.analysis.training.ModelTrainingJob
import ai.vital.aspen.groovy.predict.tasks.CollectCategoricalFeatureTaxonomyDataTask
import org.apache.spark.SparkContext
import ai.vital.vitalsigns.VitalSigns
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.vitalsigns.model.VITAL_Category
import ai.vital.predictmodel.Taxonomy
import ai.vital.vitalsigns.model.VITAL_Container
import ai.vital.vitalsigns.model.Edge_hasChildCategory
import ai.vital.aspen.groovy.featureextraction.CategoricalFeatureData

class CollectCategoricalFeatureTaxonomyDataTaskImpl(sc: SparkContext, task: CollectCategoricalFeatureTaxonomyDataTask) extends AbstractModelTrainingTaskImpl[CollectCategoricalFeatureTaxonomyDataTask](sc, task) {
  
  def checkDependencies(): Unit = {
    val trainRDD = ModelTrainingJob.getDataset(task.datasetName)       
  }

  def execute(): Unit = {
    val aspenModel = task.getModel
      
    val categoricalFeature = task.categoricalFeature
          
    val trainRDD = ModelTrainingJob.getDataset(task.datasetName)       
                
    //gather target categories
            
    val categoriesRDD = trainRDD.map { pair =>
      
      val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
                
      val vitalBlock = new VitalBlock(inputObjects)
                
      val ex = aspenModel.getFeatureExtraction
                
      val featuresMap = ex.extractFeatures(vitalBlock)

      val category = featuresMap.get(categoricalFeature.getName)
                
      if(category == null) throw new RuntimeException("No category returned: " + pair._1)
            
      val c = category.asInstanceOf[VITAL_Category]
            
      (c.getURI, c)
    }
                
    val categories = categoriesRDD.reduceByKey( (c1: VITAL_Category , c2: VITAL_Category) => c1 ).map(p => p._2).collect()
            
    println("categories count: " + categories.size)
                
    val taxonomy = new Taxonomy()
            
    val rootCategory = new VITAL_Category()
    rootCategory.setURI("urn:taxonomy-root")
    rootCategory.setProperty("name", "Taxonomy Root")
            
    var container = new VITAL_Container()
    container.putGraphObject(rootCategory)
            
    taxonomy.setRootCategory(rootCategory)
    taxonomy.setRoot(rootCategory.getURI)
    taxonomy.setContainer(container)
            
    var c = 0
    for(x <- categories) {
      c = c+1
      container.putGraphObject(x)
      val edge = new Edge_hasChildCategory()
      edge.addSource(rootCategory).addDestination(x).setURI("urn:Edge_hasChildCategory_" + rootCategory.getURI + "_" + c)
      container.putGraphObject(edge)
    }
            
    val trainedCategories = CategoricalFeatureData.fromTaxonomy(taxonomy)

    aspenModel.getFeaturesData.put(categoricalFeature.getName, trainedCategories)
            
    ModelTrainingJob.globalContext.put(categoricalFeature.getName + CollectCategoricalFeatureTaxonomyDataTask.CATEGORICAL_FEATURE_DATA_SUFFIX, trainedCategories)
    
  }
  
}