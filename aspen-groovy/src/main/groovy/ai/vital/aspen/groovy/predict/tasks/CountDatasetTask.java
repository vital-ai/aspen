package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;

/**
 * Implementation must return the training set documents count
 * @author Derek
 *
 */
public class CountDatasetTask extends ModelTrainingTask {

	public static final String DOCS_COUNT_SUFFIX = "-docs-count";

	public String datasetName;
	
	public CountDatasetTask(AspenModel model, Map<String, Object> paramsMap, String datasetName) {
		super(model, paramsMap);
		this.datasetName = datasetName;
	}


	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(datasetName);
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(datasetName + DOCS_COUNT_SUFFIX);
	}

}
