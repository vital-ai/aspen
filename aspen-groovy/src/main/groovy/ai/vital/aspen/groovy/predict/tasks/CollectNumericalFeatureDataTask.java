package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;
import ai.vital.predictmodel.NumericalFeature;

public class CollectNumericalFeatureDataTask extends ModelTrainingTask {

	public final static String NUMERICAL_FEATURE_DATA_SUFFIX = "-numerical-feature-data";
	
	public NumericalFeature feature;
	
	public String datasetName;
	
	public CollectNumericalFeatureDataTask(AspenModel model, Map<String, Object> paramsMap, NumericalFeature f, String datasetName) {
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
		return Arrays.asList(feature.getName() + NUMERICAL_FEATURE_DATA_SUFFIX);
	}

	

}
