package ai.vital.aspen.groovy.predict.tasks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;
import ai.vital.predictmodel.Aggregate;

public class CalculateAggregationValueTask extends ModelTrainingTask {

	public final static String AGGREGATE_VALUE_SUFFIX = "-aggregate-value";
	
	public Aggregate aggregate;
	
	public String datasetName;
	
	public CalculateAggregationValueTask(AspenModel model, Map<String, Object> paramsMap, Aggregate aggregate, String datasetName) {
		super(model, paramsMap);
		this.aggregate = aggregate;
		this.datasetName = datasetName;
	}

	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(datasetName);
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(aggregate.getProvides() + AGGREGATE_VALUE_SUFFIX);
	}

	
}
