package ai.vital.aspen.groovy.predict.tasks;

import java.util.List;

import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class CollectTargetCategoriesTask implements ModelTrainingTask {

	public Boolean nonCategoricalPredictions = null;
	
	public List<String> categories = null;
	
	@Override
	public void validateResult() {
		
		if(nonCategoricalPredictions != null && nonCategoricalPredictions.booleanValue()) {
			//non categorical predictions model - either regression or clsutering?
		} else {
			if(categories == null) throw new NullPointerException("Categories list must not be null");
			if(categories.size() < 2) throw new RuntimeException("Categories list must contain at least 2 different categories");
		}
		
	}

}
