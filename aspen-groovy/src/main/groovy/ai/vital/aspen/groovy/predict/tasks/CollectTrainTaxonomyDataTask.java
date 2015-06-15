package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;
import ai.vital.predictmodel.Taxonomy;
import ai.vital.predictmodel.TrainFeature;

public class CollectTrainTaxonomyDataTask extends ModelTrainingTask {

	public final static String TRAIN_TAXONOMY_DATA = "train-taxonomy-data";
	
	public TrainFeature trainFeature;
	
	public String datasetName;

	public Taxonomy taxonomy;

	public CollectTrainTaxonomyDataTask(AspenModel model, Taxonomy taxonomy, Map<String, Object> paramsMap, TrainFeature trainFeature, String datasetName) {
		super(model, paramsMap);
		this.taxonomy = taxonomy;
		this.trainFeature = trainFeature;
		this.datasetName = datasetName;
	}

	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(datasetName);
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(TRAIN_TAXONOMY_DATA);
	}
	

	

}
