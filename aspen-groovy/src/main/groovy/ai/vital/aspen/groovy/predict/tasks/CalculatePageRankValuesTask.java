package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class CalculatePageRankValuesTask extends ModelTrainingTask {

	public String inputDatasetName;
	
	public final static String NODE_URI_2_RANK = "node-uri-2-rank";
	
	public CalculatePageRankValuesTask(AspenModel model,
			Map<String, Object> globalParameters, String inputDatasetName) {
		super(model, globalParameters);
		this.inputDatasetName = inputDatasetName;
	}

	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(inputDatasetName);
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(NODE_URI_2_RANK);
	}

}
