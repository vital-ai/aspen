package ai.vital.aspen.groovy.data.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.task.AbstractTask;

public class SplitDatasetTask extends AbstractTask {

	public String inputDatasetName;
	
	public String outputDatasetName1;
	
	public String outputDatasetName2;
	
	public Double firstSplitRatio;
	
	
	public SplitDatasetTask(Map<String, Object> paramsMap, String inputDatasetName, String outputDatasetName1,
			String outputDatasetName2, Double firstSplitRatio) {
		super(paramsMap);
		this.inputDatasetName = inputDatasetName;
		this.outputDatasetName1 = outputDatasetName1;
		this.outputDatasetName2 = outputDatasetName2;
		this.firstSplitRatio = firstSplitRatio;
	}

	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(inputDatasetName);
	}



	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(outputDatasetName1, outputDatasetName2);
	}

}
