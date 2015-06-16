package ai.vital.aspen.groovy.predict;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.featureextraction.BinaryFeatureData;
import ai.vital.aspen.groovy.featureextraction.CategoricalFeatureData;
import ai.vital.aspen.groovy.featureextraction.DateFeatureData;
import ai.vital.aspen.groovy.featureextraction.DateTimeFeatureData;
import ai.vital.aspen.groovy.featureextraction.FeatureData;
import ai.vital.aspen.groovy.featureextraction.GeoLocationFeatureData;
import ai.vital.aspen.groovy.featureextraction.NumericalFeatureData;
import ai.vital.aspen.groovy.featureextraction.OrdinalFeatureData;
import ai.vital.aspen.groovy.featureextraction.StringFeatureData;
import ai.vital.aspen.groovy.featureextraction.URIFeatureData;
import ai.vital.aspen.groovy.featureextraction.WordFeatureData;
import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.modelmanager.ModelTaxonomySetter;
import ai.vital.aspen.groovy.predict.tasks.CalculateAggregationValueTask;
import ai.vital.aspen.groovy.predict.tasks.CollectCategoricalFeatureTaxonomyDataTask;
import ai.vital.aspen.groovy.predict.tasks.CollectTrainTaxonomyDataTask;
import ai.vital.aspen.groovy.predict.tasks.CollectTextFeatureDataTask;
import ai.vital.aspen.groovy.predict.tasks.CountDatasetTask;
import ai.vital.aspen.groovy.predict.tasks.LoadDataSetTask;
import ai.vital.aspen.groovy.predict.tasks.SaveModelTask;
import ai.vital.aspen.groovy.predict.tasks.SplitDatasetTask;
import ai.vital.aspen.groovy.predict.tasks.TestModelTask;
import ai.vital.aspen.groovy.predict.tasks.TrainModelTask;
import ai.vital.predictmodel.Aggregate;
import ai.vital.predictmodel.BinaryFeature;
import ai.vital.predictmodel.CategoricalFeature;
import ai.vital.predictmodel.DateFeature;
import ai.vital.predictmodel.DateTimeFeature;
import ai.vital.predictmodel.Feature;
import ai.vital.predictmodel.GeoLocationFeature;
import ai.vital.predictmodel.NumericalFeature;
import ai.vital.predictmodel.OrdinalFeature;
import ai.vital.predictmodel.PredictionModel;
import ai.vital.predictmodel.StringFeature;
import ai.vital.predictmodel.Taxonomy;
import ai.vital.predictmodel.TextFeature;
import ai.vital.predictmodel.TrainFeature;
import ai.vital.predictmodel.URIFeature;
import ai.vital.predictmodel.WordFeature;

/**
 * A class that analyzes input model and generates the tasks list required to train a model
 * @author Derek
 *
 */
public class ModelTrainingProcedure {

	//set by the upper layer
	public String inputPath;
	
	//the model being trained
	public AspenModel model;

	private ArrayList<ModelTrainingTask> tasks;
	
	
	protected String inputDatasetName = "input-dataset";
	
	protected String trainDatasetName = null; 
	protected String testDatasetName = null; 
	
	private Map<String, Object> paramsMap = null;
	
	public ModelTrainingProcedure(AspenModel model, Map<String, String> commandParams, Map<String, Object> globalParamsMap) {
		super();
		this.model = model;
		paramsMap = globalParamsMap;
		
		inputPath = commandParams.get("input");
		if(inputPath == null) throw new RuntimeException("No input procedure param");
		
	}
	
	
	
	public List<ModelTrainingTask> generateTasks() throws Exception {
		
		this.model.validateConfig();
		
//		if(model.isCategorical() && model.get)
		
		if(this.model.getAggregationResults() == null) {
			this.model.setAggregationResults(new HashMap<String, Double>());
		}
		
		if(this.model.getFeaturesData() == null) {
			this.model.setFeaturesData(new HashMap<String, FeatureData>());
		}
		
		PredictionModel cfg = this.model.getModelConfig();
		
		List<Aggregate> aggregates = cfg.getAggregates();
		
		this.tasks = new ArrayList<ModelTrainingTask>();
		
		if(inputPath == null) throw new RuntimeException("No input path set!");
		
		tasks.add(new LoadDataSetTask(model, paramsMap, inputPath, inputDatasetName));
		
		List<String> trainingRequiredParams = new ArrayList<String>();
		
		if(!model.isTestedWithTrainData()) {
			
			trainDatasetName = "train-dataset";
			testDatasetName = "test-dataset";
			
			tasks.add(new SplitDatasetTask(model, paramsMap, inputDatasetName, trainDatasetName, testDatasetName, 0.6));
			
		} else {
			
			trainDatasetName = inputDatasetName;
			testDatasetName = inputDatasetName;
			
		}
		
		trainingRequiredParams.add(trainDatasetName);
		
		
		//check if text features exist, then demand 
		
		for(Feature f : cfg.getFeatures()) {
			if(f instanceof TextFeature) {
				tasks.add(new CountDatasetTask(model, paramsMap, trainDatasetName));
				break;
			}
		}
		
		
		if(aggregates != null) {
			for(Aggregate a : aggregates) {
				tasks.add(new CalculateAggregationValueTask(model, paramsMap, a, trainDatasetName));
			}
		}
		
		
		for(Taxonomy t : cfg.getTaxonomies()) {
			
			if( t.getIntrospect() ) {
				
			}
			
		}

		
		for(Feature f : cfg.getFeatures()) {
			
			if(f instanceof BinaryFeature) {

				BinaryFeatureData bfd = new BinaryFeatureData();
				model.getFeaturesData().put(f.getName(), bfd);
				
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
				        
				if(thisTaxonomy.isIntrospect()) {
					CollectCategoricalFeatureTaxonomyDataTask ccftdt = new CollectCategoricalFeatureTaxonomyDataTask(model, thisTaxonomy, paramsMap, cf, trainDatasetName);
					tasks.add(ccftdt);
					trainingRequiredParams.addAll(ccftdt.getOutputParams());
				} else {
					CategoricalFeatureData cfd = CategoricalFeatureData.fromTaxonomy(thisTaxonomy);
					model.getFeaturesData().put(f.getName(), cfd);
				}
				
			} else if(f instanceof DateFeature) {

				DateFeatureData dfd = new DateFeatureData();
				model.getFeaturesData().put(f.getName(), dfd);
				
			} else if(f instanceof DateTimeFeature) {
				
				DateTimeFeatureData dtfd = new DateTimeFeatureData();
				model.getFeaturesData().put(f.getName(), dtfd);

			} else if(f instanceof GeoLocationFeature) {
				
				GeoLocationFeatureData glfd = new GeoLocationFeatureData();
				model.getFeaturesData().put(f.getName(), glfd);
				
			} else if(f instanceof NumericalFeature) {
				
				NumericalFeature nf = (NumericalFeature) f;
				model.getFeaturesData().put(nf.getName(), new NumericalFeatureData());
				
//				CollectNumericalFeatureDataTask cnfdt = new CollectNumericalFeatureDataTask(model, paramsMap, nf, trainDatasetName);
//				tasks.add(cnfdt);
//				trainingRequiredParams.addAll(cnfdt.getOutputParams());
				
			} else if(f instanceof OrdinalFeature) {
				
				OrdinalFeatureData ofd = new OrdinalFeatureData();
				model.getFeaturesData().put(f.getName(), ofd);
				
			} else if(f instanceof StringFeature) {
			
				StringFeatureData sfd = new StringFeatureData();
				model.getFeaturesData().put(f.getName(), sfd);
				
			} else if(f instanceof TextFeature) {
				CollectTextFeatureDataTask ctfdt = new CollectTextFeatureDataTask(model, paramsMap, (TextFeature) f, trainDatasetName);
				tasks.add(ctfdt);
				trainingRequiredParams.addAll(ctfdt.getOutputParams());
				
			} else if(f instanceof URIFeature) {
				
				URIFeatureData ufd = new URIFeatureData();
				model.getFeaturesData().put(f.getName(), ufd);
				
			} else if(f instanceof WordFeature) {
				
				WordFeature wf = (WordFeature) f;
				model.getFeaturesData().put(wf.getName(), new WordFeatureData());
				
			}
		}
		
		
		
		if(model.getTrainFeatureType().equals(CategoricalFeature.class)) {
			
			TrainFeature tf = model.getModelConfig().getTrainFeature();
			
			if(tf.getType().equals(CategoricalFeature.class)) {
			} else {
				throw new RuntimeException("Categorical models train feature is expected to be of either categorical type");
			}

			Taxonomy taxonomy = null;
			
			for(Taxonomy t : model.getModelConfig().getTaxonomies()) {
				
				if(t.getProvides().equals(tf.getTaxonomy())) {
					taxonomy = t;
				}
				
			}
			
			if(taxonomy == null) throw new RuntimeException("Taxonomy not found: " + tf.getTaxonomy());
			
			if(taxonomy.isIntrospect()) {
				CollectTrainTaxonomyDataTask cttdt = new CollectTrainTaxonomyDataTask(model, taxonomy, paramsMap, tf, trainDatasetName);
				tasks.add(cttdt);
				trainingRequiredParams.addAll(cttdt.getOutputParams());
			} else {
				model.setTrainedCategories(CategoricalFeatureData.fromTaxonomy(taxonomy));
			}
			
			
		}
		
		
		
		tasks.add(new TrainModelTask(model, paramsMap, trainDatasetName, model.getModelConfig().getType(),trainingRequiredParams));
		
		
		if(testDatasetName != null) tasks.add(new TestModelTask(model, paramsMap, testDatasetName));
		
		tasks.add(new SaveModelTask(model, paramsMap));
		
		return tasks;
		
	}
	
	
}
