package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class SplitDatasetTask extends ModelTrainingTask {

	public String inputDatasetName;
	
	public String outputDatasetName1;
	
	public String outputDatasetName2;
	
	public Double firstSplitRatio;
	
	
	public SplitDatasetTask(AspenModel model, Map<String, Object> paramsMap, String inputDatasetName, String outputDatasetName1,
			String outputDatasetName2, Double firstSplitRatio) {
		super(model, paramsMap);
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
