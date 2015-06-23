package ai.vital.aspen.groovy.data.tasks;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.task.AbstractTask;

public class ResolveURIReferencesTask extends AbstractTask {

	public String datasetName;
	
	public String modelPath;
	
	public ResolveURIReferencesTask(Map<String, Object> globalParameters, String datasetName) {
		super(globalParameters);
		this.datasetName = datasetName;
	}


	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(datasetName);
	}

	@Override
	public List<String> getOutputParams() {
		return Collections.emptyList();
	}
	
}
