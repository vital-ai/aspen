package ai.vital.aspen.groovy.predict.tasks;

import ai.vital.aspen.groovy.predict.ModelTrainingTask;

/**
 * Implementation must return the training set documents count
 * @author Derek
 *
 */
public class CountDatasetTask implements ModelTrainingTask {

	public String datasetName;
	
	public CountDatasetTask(String datasetName) {
		super();
		this.datasetName = datasetName;
	}

	public Integer result = null;

	@Override
	public void validateResult() {
		
		if(datasetName == null) throw new RuntimeException("No datasetName set");
		
		CountDatasetTask ctst = this;
		if(ctst.result == null || ctst.result < 0) throw new RuntimeException("Documents count must not be lesser than 0!");
		if(ctst.result < 2) throw new RuntimeException("Cannot progress, training documents count is lesser than 2");		
	}
	
}
