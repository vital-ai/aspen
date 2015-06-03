package ai.vital.aspen.groovy.predict.tasks;

import java.util.Map;

import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class TestModelTask extends ModelTrainingTask {

	public String datasetName;
	
	public TestModelTask(Map<String, Object> paramsMap, String datasetName) {
		super(paramsMap);
		this.datasetName = datasetName;
	}

	
	
	@Override
	public void checkDepenedencies() {
		
		if(datasetName == null) throw new NullPointerException("No datasetName set");
		
	}



	@Override
	public void onTaskComplete() {
		
	}


}
