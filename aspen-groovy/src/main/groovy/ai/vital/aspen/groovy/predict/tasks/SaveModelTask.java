package ai.vital.aspen.groovy.predict.tasks;

import java.util.Map;

import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class SaveModelTask extends ModelTrainingTask {

	public SaveModelTask(Map<String, Object> globalParameters) {
		super(globalParameters);
	}

	@Override
	public void checkDepenedencies() {
		
	}

	@Override
	public void onTaskComplete() {
		
	}


}
