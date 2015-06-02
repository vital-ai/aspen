package ai.vital.aspen.groovy.predict;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import ai.vital.aspen.groovy.featureextraction.CategoricalFeatureData;
import ai.vital.aspen.groovy.featureextraction.FeatureData;
import ai.vital.aspen.groovy.featureextraction.NumericalFeatureData;
import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.tasks.CalculateAggregationValueTask;
import ai.vital.aspen.groovy.predict.tasks.CollectCategoricalFeaturesDataTask;
import ai.vital.aspen.groovy.predict.tasks.CollectNumericalFeatureDataTask;
import ai.vital.aspen.groovy.predict.tasks.CollectTargetCategoriesTask;
import ai.vital.aspen.groovy.predict.tasks.CollectTextFeatureDataTask;
import ai.vital.aspen.groovy.predict.tasks.CountTrainingSetTask;
import ai.vital.aspen.groovy.predict.tasks.ProvideMinDFMaxDF;
import ai.vital.aspen.groovy.predict.tasks.SaveModelTask;
import ai.vital.aspen.groovy.predict.tasks.TrainModelTask;
import ai.vital.predictmodel.Aggregate;
import ai.vital.predictmodel.CategoricalFeature;
import ai.vital.predictmodel.Feature;
import ai.vital.predictmodel.NumericalFeature;
import ai.vital.predictmodel.PredictionModel;
import ai.vital.predictmodel.Taxonomy;
import ai.vital.predictmodel.TextFeature;

/**
 * A class that analyzes input model and generates the tasks list required to train a model
 * @author Derek
 *
 */
public class ModelTrainingProcedure {

	//the model being trained
	public AspenModel model;

	public Integer trainingDocsCount = null;
//	public Integer testingDocsCount = null;

	private ArrayList<ModelTrainingTask> tasks;
	
	public Integer minDF = null;
	public Integer maxDF = null;
	
	public ModelTrainingProcedure(AspenModel model) {
		super();
		
		this.model = model;
		
	}
	
	ModelTrainingTask currentTask = null;
	
	Iterator<ModelTrainingTask> iterator = null;
	
	public ModelTrainingTask getNextTask() {
		
		if(tasks == null) throw new RuntimeException("Tasks list not generated!");
		
		if(currentTask != null) throw new RuntimeException("Task in progress: " + currentTask);
		
		if(iterator == null) iterator = tasks.iterator();
		
		if(iterator.hasNext()) {
			currentTask = iterator.next();
			return currentTask;
		}
		
		return null;
		
	}
	
	
	
	public List<ModelTrainingTask> generateTasks() throws Exception {
		
		this.model.validateConfig();
		
		if(this.model.getAggregationResults() == null) {
			this.model.setAggregationResults(new HashMap<String, Double>());
		}
		
		if(this.model.getFeaturesData() == null) {
			this.model.setFeaturesData(new HashMap<String, FeatureData>());
		}
		
		PredictionModel cfg = this.model.getModelConfig();
		
		List<Aggregate> aggregates = cfg.getAggregates();
		
		this.tasks = new ArrayList<ModelTrainingTask>();
		
		tasks.add(new CollectTargetCategoriesTask());
		
		//check if text features exist, then demand 
		
		for(Feature f : cfg.getFeatures()) {
			if(f instanceof TextFeature) {
				tasks.add(new CountTrainingSetTask());
				tasks.add(new ProvideMinDFMaxDF());
				break;
			}
		}
		
		
		if(aggregates != null) {
			for(Aggregate a : aggregates) {
				tasks.add(new CalculateAggregationValueTask(a));
			}
		}

		
		for(Feature f : cfg.getFeatures()) {
			if(f instanceof TextFeature) {
				tasks.add(new CollectTextFeatureDataTask((TextFeature) f));
			} else if(f instanceof CategoricalFeature) {

				CategoricalFeature cf = (CategoricalFeature) f;
				//just get it from model ? 
				
				//conditional
		        //use default
				Taxonomy thisTaxonomy = null;
				for( Taxonomy t : model.getModelConfig().getTaxonomies() ) {
					if(t.getProvides().equals(cf.getTaxonomy())) {
						thisTaxonomy = t;
					}
				}
				
				if(thisTaxonomy == null) throw new RuntimeException("Taxonomy not found: " + cf.getTaxonomy());
				        
				CategoricalFeatureData cfd = CategoricalFeatureData.fromTaxonomy(thisTaxonomy);
				        
				model.getFeaturesData().put(f.getName(), cfd);
//				tasks.add(new CollectCategoricalFeaturesDataTask(cf));
				
			} else if(f instanceof NumericalFeature) {
				
				NumericalFeature nf = (NumericalFeature) f;
				
//				tasks.add(new CollectNumericalFeatureDataTask(nf));
				model.getFeaturesData().put(nf.getName(), new NumericalFeatureData());
				
			}
		}
		
		tasks.add(new TrainModelTask());
		
		tasks.add(new SaveModelTask());
		
		return tasks;
	}
	
	public void onTaskComplete(ModelTrainingTask task) {
	
		if(task == null) throw new NullPointerException("Task must not be null");
		
		if(currentTask == null) throw new RuntimeException("No active task!");
		
		if(task != currentTask) throw new RuntimeException("Complete task is different that ");
		
		task.validateResult();
		
		if(task instanceof CalculateAggregationValueTask) {
		
			CalculateAggregationValueTask cavt = (CalculateAggregationValueTask) task;
			Aggregate a = cavt.aggregate;
			model.getAggregationResults().put(a.getProvides(), cavt.value);
			
		} else if(task instanceof CollectCategoricalFeaturesDataTask) {
			throw new RuntimeException("Disconnected task: " + task.getClass().getCanonicalName());
		} else if(task instanceof CollectNumericalFeatureDataTask) {
			throw new RuntimeException("Disconnected task: " + task.getClass().getCanonicalName());
		} else if(task instanceof CollectTargetCategoriesTask) {
			
			CollectTargetCategoriesTask ctct = (CollectTargetCategoriesTask) task;
			if(ctct.nonCategoricalPredictions != null && ctct.nonCategoricalPredictions.booleanValue()) {
				
			} else {
				
				CategoricalFeatureData cfd = new CategoricalFeatureData();
				cfd.setCategories(ctct.categories);
				model.setTrainedCategories(cfd);
				
			}
			
		} else if(task instanceof CollectTextFeatureDataTask) {
			
			CollectTextFeatureDataTask ctfdt = (CollectTextFeatureDataTask) task;
			
	        model.getFeaturesData().put(ctfdt.feature.getName(), ctfdt.results);
			
		} else if(task instanceof CountTrainingSetTask) {
			
			CountTrainingSetTask ctst = (CountTrainingSetTask) task;
			trainingDocsCount = ctst.result;
			
		} else if(task instanceof ProvideMinDFMaxDF) {
			
			ProvideMinDFMaxDF pmm = (ProvideMinDFMaxDF) task;
			minDF = pmm.minDF;
			maxDF = pmm.maxDF;
			
		} else if(task instanceof SaveModelTask) {
			
			//do nothing
		} else if(task instanceof TrainModelTask) {
			
		} else {
			throw new RuntimeException("Unhandled task completion type: " + task.getClass().getCanonicalName());
		}
		
		currentTask = null;
		
	}
	
	
	
}
