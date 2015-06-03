package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class SaveModelTask extends ModelTrainingTask {

	public SaveModelTask(AspenModel model, Map<String, Object> globalParameters) {
		super(model, globalParameters);
	}

	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(TrainModelTask.MODEL_BINARY);
	}

	@Override
	public List<String> getOutputParams() {
		return Collections.emptyList();
	}

}
