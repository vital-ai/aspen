package ai.vital.aspen.groovy.predict;

import java.util.Map;


public abstract class ModelTrainingTask {
	
	protected Map<String, Object> paramsMap;
	
	public ModelTrainingTask(Map<String, Object> globalParameters) {
		this.paramsMap = globalParameters;
	}
	
	public Map<String, Object> getParamsMap() {
		return paramsMap;
	}

	//called when the task is about to be executed
	public abstract void checkDepenedencies();
	
	//called when the task is complete and should update the parameters map with results
	public abstract void onTaskComplete();
	
}
