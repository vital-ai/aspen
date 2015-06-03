package ai.vital.aspen.groovy.predict.tasks;

import ai.vital.aspen.groovy.predict.ModelTrainingTask;
import ai.vital.predictmodel.Aggregate;

public class CalculateAggregationValueTask implements ModelTrainingTask {

	public Aggregate aggregate;
	
	public String datasetName;
	
	public Double value;

	
	public CalculateAggregationValueTask(Aggregate aggregate, String datasetName) {
		super();
		this.aggregate = aggregate;
		this.datasetName = datasetName;
	}


	@Override
	public void validateResult() {

		if(aggregate == null) throw new NullPointerException("aggregation not set!");
		if(datasetName == null) throw new NullPointerException("datasetName not set!");
		
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
		
	}
	
}
