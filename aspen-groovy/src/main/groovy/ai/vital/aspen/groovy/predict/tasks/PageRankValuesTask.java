package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class PageRankValuesTask extends ModelTrainingTask {

	public String inputDatasetName;
	public String outputDatasetName;
	
	public PageRankValuesTask(AspenModel model,
			Map<String, Object> globalParameters, String inputDatasetName, String outputDatasetName) {
		super(model, globalParameters);
		this.inputDatasetName = inputDatasetName;
		this.outputDatasetName = outputDatasetName;
	}

	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(inputDatasetName);
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(outputDatasetName);
	}

}
