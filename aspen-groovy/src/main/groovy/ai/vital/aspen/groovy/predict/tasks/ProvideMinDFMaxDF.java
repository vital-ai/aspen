package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class ProvideMinDFMaxDF extends ModelTrainingTask {

	public final static String MIN_DF_SUFFIX = "-min-df";
	
	public final static String MAX_DF_SUFFIX = "-max-df";
	
	public String datasetName;

	public ProvideMinDFMaxDF(AspenModel model, Map<String, Object> paramsMap, String datasetName) {
		super(model, paramsMap);
		this.datasetName = datasetName;
	}

	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(datasetName + CountDatasetTask.DOCS_COUNT_SUFFIX);
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(datasetName + MIN_DF_SUFFIX, datasetName + MAX_DF_SUFFIX);
	}

	


}
