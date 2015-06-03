package ai.vital.aspen.groovy.predict.tasks;

import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class ProvideMinDFMaxDF implements ModelTrainingTask {

	public String datasetName;

	public Integer docsCount;
	
	public Integer minDF;
	
	public Integer maxDF;
	
	public ProvideMinDFMaxDF(String datasetName, Integer docsCount) {
		super();
		this.datasetName = datasetName;
		this.docsCount = docsCount;
	}

	@Override
	public void validateResult() {

		if(datasetName == null) throw new NullPointerException("No datasetName set");
		
		if(minDF == null) throw new NullPointerException("No minDF");
		
		if(maxDF == null) throw new NullPointerException("No maxDF");
		
	}

}
