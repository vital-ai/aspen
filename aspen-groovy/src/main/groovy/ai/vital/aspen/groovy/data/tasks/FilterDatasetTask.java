package ai.vital.aspen.groovy.data.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.task.AbstractTask;

public class FilterDatasetTask extends AbstractTask {

	public final static String FILTER_DATASET_STATS = "filter-dataset-stats";
	
	public String inputDatasetName;
	
	public String queryBuilderPath;
	
	public String outputDatasetName;
	
	public FilterDatasetTask(Map<String, Object> globalParameters, String inputDatasetName, String outputDatasetName, String queryBuilderPath) {
		super(globalParameters);
		this.inputDatasetName = inputDatasetName;
		this.outputDatasetName = outputDatasetName;
		this.queryBuilderPath = queryBuilderPath;
		
	}

	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(inputDatasetName);
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(outputDatasetName, FILTER_DATASET_STATS);
	}

}
