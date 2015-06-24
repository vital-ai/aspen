package ai.vital.aspen.groovy.predict;

import groovy.lang.GString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.convert.tasks.CheckPathTask;
import ai.vital.aspen.groovy.convert.tasks.ConvertSequenceToBlockTask;
import ai.vital.aspen.groovy.convert.tasks.DeletePathTask;
import ai.vital.aspen.groovy.data.tasks.LoadDataSetTask;
import ai.vital.aspen.groovy.data.tasks.SaveDataSetTask;
import ai.vital.aspen.groovy.data.tasks.SplitDatasetTask;
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
import ai.vital.aspen.groovy.predict.tasks.AssignPageRankValuesTask;
import ai.vital.aspen.groovy.predict.tasks.CalculateAggregationValueTask;
import ai.vital.aspen.groovy.predict.tasks.CalculatePageRankValuesTask;
import ai.vital.aspen.groovy.predict.tasks.CollectCategoricalFeatureTaxonomyDataTask;
import ai.vital.aspen.groovy.predict.tasks.CollectTextFeatureDataTask;
import ai.vital.aspen.groovy.predict.tasks.CollectTrainTaxonomyDataTask;
import ai.vital.aspen.groovy.predict.tasks.CountDatasetTask;
import ai.vital.aspen.groovy.predict.tasks.FeatureQueryTask;
import ai.vital.aspen.groovy.predict.tasks.LoadModelTask;
import ai.vital.aspen.groovy.predict.tasks.SaveModelTask;
import ai.vital.aspen.groovy.predict.tasks.TestModelTask;
import ai.vital.aspen.groovy.predict.tasks.TrainModelTask;
import ai.vital.aspen.groovy.task.AbstractTask;
import ai.vital.predictmodel.Aggregate;
import ai.vital.predictmodel.AlgorithmConfig;
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

	private ArrayList<AbstractTask> tasks;
	
	
	protected String inputDatasetName = "input-dataset";
	
	protected String trainDatasetName = null; 
	protected String testDatasetName = null; 
	
	private Map<String, Object> paramsMap = null;
	
	private String modelPath = null;

	private boolean overwriteModel = false;
	
	public ModelTrainingProcedure(AspenModel model, String inputPath, String modelPath, boolean overwriteModel, Map<String, Object> globalParamsMap) {
		super();
		this.model = model;
		paramsMap = globalParamsMap;
		
		this.inputPath = inputPath;
		this.modelPath = modelPath;
		this.overwriteModel = overwriteModel;
		
		if(inputPath == null) throw new RuntimeException("No input procedure param");
		
	}
	
	
	
	public List<AbstractTask> generateTasks() throws Exception {
		
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
		
		this.tasks = new ArrayList<AbstractTask>();
		
		if(inputPath == null) throw new RuntimeException("No input path set!");
		
		if(inputDatasetName.startsWith("name:")) {

			inputDatasetName = inputDatasetName.substring(5);
			
			paramsMap.put(inputDatasetName, true);
			
		} else {
			
			tasks.add(new LoadDataSetTask(paramsMap, inputPath, inputDatasetName));
			
		}
		
		
		if(overwriteModel) {
			
			tasks.add(new DeletePathTask(modelPath, paramsMap));
			
		} else {
			
			CheckPathTask cpt = new CheckPathTask(modelPath, paramsMap);
			cpt.mustnotExist = true;
			cpt.mustExist = false;
			cpt.acceptFiles = true;
			cpt.acceptFiles = true;
			
			tasks.add(cpt);
			
		}
		
		
		//treat it as a loaded model
		paramsMap.put(LoadModelTask.LOADED_MODEL_PREFIX + modelPath, model);
		tasks.add(new FeatureQueryTask(paramsMap, inputDatasetName, modelPath));
		
		
		List<String> trainingRequiredParams = new ArrayList<String>();
		
		if(!model.isTestedWithTrainData()) {
			
			trainDatasetName = "train-dataset";
			testDatasetName = "test-dataset";
			
			tasks.add(new SplitDatasetTask(paramsMap, inputDatasetName, trainDatasetName, testDatasetName, 0.6));
			
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
		
		
		//load data with feature queries, treat model as not fully loaded
//		tasks.add(new FeatureQueryTask(paramsMap, trainDatasetName, modelPath));
		
		
		String outputDatasetName = null;
		String outputPath = null;
		
		if("spark-page-rank-prediction".equals(model.getType())) {
			
			AlgorithmConfig ac = model.getModelConfig().getAlgorithmConfig();
			if(ac == null) throw new RuntimeException("Algorithm config in page rank model is required");
			Serializable outputPathParam = ac.get("outputPath");
			if(outputPathParam == null) throw new RuntimeException("outputPath algoritm config value in page rank model is required");
			if(!(outputPathParam instanceof String || outputPathParam instanceof GString)) throw new RuntimeException("outputPath must be a string");
			outputPath = outputPathParam.toString();
			
			if(outputPath.startsWith("name:")) {
				
				outputDatasetName = outputPath.substring(5);
				
			} else {
				
				outputDatasetName = "page-rank-training-output";
				
				
			}
			
			tasks.add(new DeletePathTask("name:" + outputDatasetName, paramsMap));
			tasks.add(new DeletePathTask(outputPath, paramsMap));
			
		}
				
		//page rank special
		
		
		
		
		if(outputDatasetName == null) {
			
			tasks.add(new TrainModelTask(model, modelPath, paramsMap, trainDatasetName, model.getModelConfig().getType(), trainingRequiredParams));
			
			if(testDatasetName != null) {
				
//				if(!trainDatasetName.equals(testDatasetName)) {
//					tasks.add(new FeatureQueryTask(paramsMap, testDatasetName, modelPath));
//				}
				
				tasks.add(new TestModelTask(model, paramsMap, testDatasetName));
			}
			
			tasks.add(new SaveModelTask(model, modelPath, paramsMap));
			
		} else {
			
			
			tasks.add(new CalculatePageRankValuesTask(model, paramsMap, inputDatasetName));
			
			tasks.add(new AssignPageRankValuesTask(model, paramsMap, inputDatasetName, outputDatasetName));
			
			//persist the dataset
		
			if(outputPath.startsWith("name:")) {
				
				//do nothing
				
			} else if(outputPath.endsWith(".vital.seq")) {
				
				tasks.add(new SaveDataSetTask(paramsMap, outputDatasetName, outputPath));
				
			} else if(outputPath.endsWith(".vital") || outputPath.endsWith(".vital.gz")) {
				
				//dump it first
				String datasetSequencePath = AspenGroovyConfig.get().datesetsLocation + "/" + outputDatasetName+ "/" + outputDatasetName + ".vital.seq";
				
				tasks.add(new DeletePathTask(datasetSequencePath, paramsMap));
				tasks.add(new SaveDataSetTask(paramsMap, outputDatasetName, datasetSequencePath));
				
				//convert
				tasks.add(new ConvertSequenceToBlockTask(Arrays.asList(datasetSequencePath), outputPath, paramsMap));
				
			} else {
				throw new RuntimeException("Unexcepted outputPath value: " + outputPath);
			}
			 
		}
		
		return tasks;
		
	}
	
	
}
