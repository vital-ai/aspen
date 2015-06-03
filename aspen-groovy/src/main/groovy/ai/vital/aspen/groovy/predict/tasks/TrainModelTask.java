package ai.vital.aspen.groovy.predict.tasks;

import java.io.Serializable;
import java.util.Map;

import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class TrainModelTask extends ModelTrainingTask {

	public String datasetName;
	
	public String algorithm;
	
	//result
	public Serializable modelBinary;

	
	public TrainModelTask(Map<String, Object> paramsMap, String datasetName, String algorithm) {
		super(paramsMap);
		this.datasetName = datasetName;
		this.algorithm = algorithm;
	}


	
	
	@Override
	public void checkDepenedencies() {
		
	}



	@Override
	public void onTaskComplete() {

		if(modelBinary == null) throw new NullPointerException("No model binary set!");
		
	}

}
