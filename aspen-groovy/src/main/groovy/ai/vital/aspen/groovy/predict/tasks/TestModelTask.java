package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class TestModelTask extends ModelTrainingTask {

	public final static String STATS_STRING = "test-model-stats-string";
	
	public String datasetName;
	
	public TestModelTask(AspenModel model, Map<String, Object> paramsMap, String datasetName) {
		super(model, paramsMap);
		this.datasetName = datasetName;
	}

	
	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(datasetName, TrainModelTask.MODEL_BINARY);
	}



	@Override
	public List<String> getOutputParams() {
		return Collections.emptyList();
	}


}
