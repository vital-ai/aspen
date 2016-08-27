package ai.vital.aspen.data;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ai.vital.aspen.groovy.task.AbstractTask;

public class DataProfileTask extends AbstractTask {

	public final static String DATASET_STATS = "DataProfileProcedure_DATASET_STATS";
	
	private String inputDatasetName;
	private String outputPath;
	private Set<String> propertiesFilter;
	
	public DataProfileTask(Map<String, Object> globalParameters, String inputDatasetName, String outputPath, Set<String> propertiesFilter) {
		super(globalParameters);
		this.inputDatasetName = inputDatasetName;
		this.outputPath = outputPath;
		this.propertiesFilter = propertiesFilter;
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(DATASET_STATS);		
	}

	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(inputDatasetName);		
	}

	public String getInputDatasetName() {
		return inputDatasetName;
	}

	public void setInputDatasetName(String inputDatasetName) {
		this.inputDatasetName = inputDatasetName;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public Set<String> getPropertiesFilter() {
		return propertiesFilter;
	}

	public void setPropertiesFilter(Set<String> propertiesFilter) {
		this.propertiesFilter = propertiesFilter;
	}

}
