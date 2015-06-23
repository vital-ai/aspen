package ai.vital.aspen.groovy.data.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.task.AbstractTask;

public class SaveDataSetTask extends AbstractTask {

	public final static String SAVE_DATASET_PREFIX = "saved-dataset-";
	
	public String datasetName;
	
	public String outputPath;
	
	public SaveDataSetTask(Map<String, Object> globalParameters, String datasetName, String outputPath) {
		super(globalParameters);
		this.datasetName = datasetName;
		this.outputPath = outputPath;
	}

	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(datasetName);
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(SAVE_DATASET_PREFIX + datasetName);
	}

}
