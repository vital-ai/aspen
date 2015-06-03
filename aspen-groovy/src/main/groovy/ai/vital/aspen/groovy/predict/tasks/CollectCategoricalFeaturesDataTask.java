package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;
import ai.vital.predictmodel.CategoricalFeature;

public class CollectCategoricalFeaturesDataTask extends ModelTrainingTask {

	public final static String CATEGORICAL_FEATURE_DATA_SUFFIX = "-categorical-feature-data";
	
	public CategoricalFeature feature;
	
	public String datasetName;

	public CollectCategoricalFeaturesDataTask(AspenModel model, Map<String, Object> paramsMap, CategoricalFeature f, String datasetName) {
		super(model, paramsMap);
		this.feature = f;
		this.datasetName = datasetName;
	}

	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(datasetName);
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(feature.getName() + CATEGORICAL_FEATURE_DATA_SUFFIX);
	}
	

	

}
