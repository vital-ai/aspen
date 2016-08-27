package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.task.AbstractTask;

public class ModelPredictTask extends AbstractTask {


	public final static String STATS_STRING = "model-predict-task-stats";
	
	public String inputDatasetName;
	
	public String outputDatasetName;
	
	public String modelPath;
	
	public ModelPredictTask(Map<String, Object> globalParameters, String inputDatasetName, String outputDatasetName, String modelPath) {
		super(globalParameters);
		this.inputDatasetName = inputDatasetName;
		this.outputDatasetName = outputDatasetName;
		this.modelPath = modelPath;
	}
	
	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(inputDatasetName, LoadModelTask.LOADED_MODEL_PREFIX + modelPath);
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(STATS_STRING, outputDatasetName);
	}

}
