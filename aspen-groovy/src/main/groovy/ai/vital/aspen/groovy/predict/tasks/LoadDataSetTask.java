package ai.vital.aspen.groovy.predict.tasks;

import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class LoadDataSetTask implements ModelTrainingTask {

	public String path;
	
	public String datasetName;
	
	
	//result
	public Boolean loaded = null;
	
	public LoadDataSetTask(String path, String datasetName) {
		super();
		this.path = path;
		this.datasetName = datasetName;
	}



	@Override
	public void validateResult() {

		if(path == null) throw new NullPointerException("Null path");
		
		if(datasetName == null) throw new NullPointerException("Null datasetName");

		if(loaded == null) throw new NullPointerException("Loaded flag not set");
		if(!loaded.booleanValue()) throw new RuntimeException("Dataset not loaded: " + datasetName);
	}

}
