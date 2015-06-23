package ai.vital.aspen.groovy.data.tasks;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.task.AbstractTask;

public class LoadDataSetTask extends AbstractTask {

	//vital sequence file
	public String path;
	
	public String datasetName;
	
	public LoadDataSetTask(Map<String, Object> paramsMap, String path, String datasetName) {
		super(paramsMap);
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
