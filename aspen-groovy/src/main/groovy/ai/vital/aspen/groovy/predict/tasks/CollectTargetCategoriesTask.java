package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class CollectTargetCategoriesTask extends ModelTrainingTask {

	public final static String TARGET_CATEGORIES_DATA = "target-categories-data";
	
	public String datasetName;
	
	public CollectTargetCategoriesTask(AspenModel model, Map<String, Object> paramsMap, String datasetName) {
		super(model, paramsMap);
		this.datasetName = datasetName;
	}

	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(datasetName);
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(TARGET_CATEGORIES_DATA);
	}

}
