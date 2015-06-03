package ai.vital.aspen.groovy.predict.tasks;

import java.io.Serializable;

import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class TrainModelTask implements ModelTrainingTask {

	public String datasetName;
	
	public String algorithm;
	
	
	public TrainModelTask(String datasetName, String algorithm) {
		super();
		this.datasetName = datasetName;
		this.algorithm = algorithm;
	}

	//result
	public Serializable modelBinary;
	
	@Override
	public void validateResult() {

		if(modelBinary == null) throw new NullPointerException("No model binary set!");
		
	}

}
