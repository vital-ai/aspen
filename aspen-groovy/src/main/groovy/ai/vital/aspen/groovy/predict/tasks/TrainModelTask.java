package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class TrainModelTask extends ModelTrainingTask {

	public final static String MODEL_BINARY = "model-binary";
	
	public String datasetName;
	
	public String algorithm;
	
	public List<String> requiredParams;
	
	public TrainModelTask(AspenModel model, Map<String, Object> paramsMap, String datasetName, String algorithm, List<String> inputFeatures) {
		super(model, paramsMap);
		this.datasetName = datasetName;
		this.algorithm = algorithm;
		this.requiredParams = inputFeatures;
	}

	@Override
	public List<String> getRequiredParams() {
		return requiredParams;
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(MODEL_BINARY);
	}

}
