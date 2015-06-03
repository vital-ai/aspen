package ai.vital.aspen.groovy.predict;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;


public abstract class ModelTrainingTask {
	
	protected Map<String, Object> paramsMap;
	protected AspenModel model;
	
	public ModelTrainingTask(AspenModel model, Map<String, Object> globalParameters) {
		this.model = model;
		this.paramsMap = globalParameters;
	}
	
	public Map<String, Object> getParamsMap() {
		return paramsMap;
	}
	
	public AspenModel getModel() {
		return model;
	}

	//called when the task is about to be executed
	public final void checkDepenedencies() {
		
		List<String> missing = null;
		
		for(String param : getRequiredParams() ) {
			if(!paramsMap.containsKey(param)) {
				if(missing == null) missing = new ArrayList<String>();
				missing.add(param);
			}
		}
		if(missing != null) {
			throw new RuntimeException("Missing input parameter" + ( missing.size() != 1 ? "s" : "") + ": " + missing);
		}

	}
	
	//called when the task is complete and should update the parameters map with results
	public final void onTaskComplete() {
		
		List<String> missing = null;
		for(String param : getOutputParams() ) {
			if(!paramsMap.containsKey(param)) {
				if(missing == null) missing = new ArrayList<String>();
				missing.add(param);
			}
		}
		if(missing != null) {
			throw new RuntimeException("Missing output parameter" + ( missing.size() != 1 ? "s" : "") + ": " + missing);
		}
		
	}
	
	/**
	 * override it if a task expects some parametrs from global context map 
	 * @return
	 */
	public abstract List<String> getRequiredParams();

	/**
	 * override it if a task returns some output parameters
	 * @return
	 */
	public abstract List<String> getOutputParams();

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + " req: " + getRequiredParams() + " out:" + getOutputParams();
	}
}
