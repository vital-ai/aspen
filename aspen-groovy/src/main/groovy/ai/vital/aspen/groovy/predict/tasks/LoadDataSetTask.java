package ai.vital.aspen.groovy.predict.tasks;

import java.util.Map;

import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class LoadDataSetTask extends ModelTrainingTask {

	public String path;
	
	public String datasetName;
	
	
	//result
	public Boolean loaded = null;
	
	public LoadDataSetTask(Map<String, Object> paramsMap, String path, String datasetName) {
		super(paramsMap);
		this.path = path;
		this.datasetName = datasetName;
	}


	

	@Override
	public void checkDepenedencies() {
		
		if(path == null) throw new NullPointerException("Null path");
		
		if(datasetName == null) throw new NullPointerException("Null datasetName");
		
	}




	@Override
	public void onTaskComplete() {

		if(loaded == null) throw new NullPointerException("Loaded flag not set");
		if(!loaded.booleanValue()) throw new RuntimeException("Dataset not loaded: " + datasetName);
		
	}

}
