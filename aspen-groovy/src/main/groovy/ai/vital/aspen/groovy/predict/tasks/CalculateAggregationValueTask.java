package ai.vital.aspen.groovy.predict.tasks;

import java.util.Map;

import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;
import ai.vital.predictmodel.Aggregate;

public class CalculateAggregationValueTask extends ModelTrainingTask {

	public Aggregate aggregate;
	
	public String datasetName;
	
	public Double value;

	private AspenModel model;

	
	public CalculateAggregationValueTask(AspenModel model, Map<String, Object> paramsMap, Aggregate aggregate, String datasetName) {
		super(paramsMap);
		this.model = model;
		this.aggregate = aggregate;
		this.datasetName = datasetName;
	}

	@Override
	public void checkDepenedencies() {
		
		if(aggregate == null) throw new NullPointerException("aggregation not set!");
		if(datasetName == null) throw new NullPointerException("datasetName not set!");

	}
	
	@Override
	public void onTaskComplete() {
		
		if(value == null) throw new RuntimeException("No aggregation value returned, " + aggregate.getProvides() + " " + aggregate.getFunction());
		
		switch (aggregate.getFunction()) {
			case AVERAGE : {
			}
			case MAX : {
				
			}
			case MIN : {
				
			}
			case SUM : {
				
			}
			default : {
				
			}
		}
		
		model.getAggregationResults().put(aggregate.getProvides(), value);
		
	}
	
}
