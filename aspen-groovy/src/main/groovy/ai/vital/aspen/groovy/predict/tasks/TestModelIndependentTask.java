package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.task.AbstractTask;

/**
 * independent from training model testing task
 *
 */
public class TestModelIndependentTask extends AbstractTask {

	public String modelPath;
	public String datasetName;

	public TestModelIndependentTask(Map<String, Object> globalParameters, String modelPath, String datasetName) {
		super(globalParameters);
		this.modelPath = modelPath;
		this.datasetName = datasetName; 
	}

	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(datasetName, LoadModelTask.LOADED_MODEL_PREFIX + modelPath);
	}

	@Override
	public List<String> getOutputParams() {
		return Collections.emptyList();
	}

}
