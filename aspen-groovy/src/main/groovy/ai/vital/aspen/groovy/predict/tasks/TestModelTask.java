package ai.vital.aspen.groovy.predict.tasks;

import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class TestModelTask implements ModelTrainingTask {

	public String datasetName;
	
	public TestModelTask(String datasetName) {
		super();
		this.datasetName = datasetName;
	}

	@Override
	public void validateResult() {

		if(datasetName == null) throw new NullPointerException("No datasetName set");
		
	}

}
