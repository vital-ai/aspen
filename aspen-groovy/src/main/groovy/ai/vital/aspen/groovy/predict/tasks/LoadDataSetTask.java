package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class LoadDataSetTask extends ModelTrainingTask {

	public String path;
	
	public String datasetName;
	
	public LoadDataSetTask(AspenModel model, Map<String, Object> paramsMap, String path, String datasetName) {
		super(model, paramsMap);
		this.path = path;
		this.datasetName = datasetName;
	}


	@Override
	public List<String> getRequiredParams() {
		return Collections.emptyList();
	}


	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(datasetName);
	}



}
