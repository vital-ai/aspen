package ai.vital.aspen.groovy.predict;

import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.task.AbstractTask;


public abstract class ModelTrainingTask extends AbstractTask {
	
	protected AspenModel model;
	
	public ModelTrainingTask(AspenModel model, Map<String, Object> globalParameters) {
		super(globalParameters);
		this.model = model;
	}
	
	public AspenModel getModel() {
		return model;
	}
	
}
