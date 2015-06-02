package ai.vital.aspen.groovy.predict.tasks;

import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class ProvideMinDFMaxDF implements ModelTrainingTask {

	public Integer minDF;
	
	public Integer maxDF;
	
	@Override
	public void validateResult() {

		if(minDF == null) throw new NullPointerException("No minDF");
		
		if(maxDF == null) throw new NullPointerException("No maxDF");
		
	}

}
