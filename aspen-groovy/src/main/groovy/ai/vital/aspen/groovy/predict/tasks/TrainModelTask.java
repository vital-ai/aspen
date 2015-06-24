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
	
	public String modelPath;

	//in page rank or other model without serialized output
	public String outputDatasetName;
	
	private List<String> requiredParams;
	
	public TrainModelTask(AspenModel model, String modelPath, Map<String, Object> paramsMap, String datasetName, String algorithm, String outputDatasetName, List<String> inputFeatures) {
		super(model, paramsMap);
		this.datasetName = datasetName;
		this.algorithm = algorithm;
		this.modelPath = modelPath;
		this.requiredParams = inputFeatures;
		this.outputDatasetName = outputDatasetName;
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
