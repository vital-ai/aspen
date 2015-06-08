package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;
import ai.vital.predictmodel.TextFeature;

public class CollectTextFeatureDataTask extends ModelTrainingTask {

	public final static String TEXT_FEATURE_DATA_SUFFIX = "-text-feature-data";
	
	public TextFeature feature;
	
	public String datasetName;

	
	public CollectTextFeatureDataTask(AspenModel model, Map<String, Object> paramsMap, TextFeature f, String datasetName) {
		super(model, paramsMap);
		this.feature = f;
		this.datasetName = datasetName;
	}

	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(
				datasetName, 
				datasetName + CountDatasetTask.DOCS_COUNT_SUFFIX 
		);
	}


	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(feature.getName() + TEXT_FEATURE_DATA_SUFFIX);
	}


}
