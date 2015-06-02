package ai.vital.aspen.groovy.predict.tasks;

import java.io.Serializable;

import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class TrainModelTask implements ModelTrainingTask {

	public Serializable modelBinary;
	
	@Override
	public void validateResult() {

		if(modelBinary == null) throw new NullPointerException("No model binary set!");
		
	}

}
