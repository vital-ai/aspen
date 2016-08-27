package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.task.AbstractTask;

/**
 * Expects either binary param
 * @author Derek
 *
 */

public class FeatureQueryTask extends AbstractTask {

	public final static String FEATURE_QUERY_PREFIX = "feature-query-";
	
	public String datasetName;
	
	public String modelPath;

	public FeatureQueryTask(Map<String, Object> globalParameters, String datasetName, String modelPath) {
		super(globalParameters);
		this.datasetName = datasetName;
		this.modelPath = modelPath;
	}

	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(datasetName, LoadModelTask.LOADED_MODEL_PREFIX + modelPath);
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(FEATURE_QUERY_PREFIX + datasetName);
	}

}
