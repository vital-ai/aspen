package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.task.AbstractTask;

public class LoadModelTask extends AbstractTask {

	public final static String LOADED_MODEL_PREFIX = "loaded-model-";
	
	public String modelPath;
	
	
	public LoadModelTask(Map<String, Object> globalParameters, String modelPath) {
		super(globalParameters);
		this.modelPath = modelPath;
	}

	@Override
	public List<String> getRequiredParams() {
		return Collections.emptyList();
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(LOADED_MODEL_PREFIX + modelPath);
	}

}
