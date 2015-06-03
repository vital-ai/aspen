package ai.vital.aspen.groovy.predict.tasks;

import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.featureextraction.CategoricalFeatureData;
import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class CollectTargetCategoriesTask extends ModelTrainingTask {

	public String datasetName;
	
	public Boolean nonCategoricalPredictions = null;
	
	public List<String> categories = null;

	private AspenModel model;
	
	public CollectTargetCategoriesTask(AspenModel model, Map<String, Object> paramsMap, String datasetName) {
		super(paramsMap);
		this.model = model;
		this.datasetName = datasetName;
	}

	
	
	@Override
	public void checkDepenedencies() {
		
		if(datasetName == null) throw new NullPointerException("No datasetName set");
		
	}



	@Override
	public void onTaskComplete() {
		
		if(nonCategoricalPredictions != null && nonCategoricalPredictions.booleanValue()) {
			//non categorical predictions model - either regression or clsutering?
		} else {
			if(categories == null) throw new NullPointerException("Categories list must not be null");
			if(categories.size() < 2) throw new RuntimeException("Categories list must contain at least 2 different categories");
			
			CategoricalFeatureData cfd = new CategoricalFeatureData();
			cfd.setCategories(categories);
			model.setTrainedCategories(cfd);
			
		}
	}

}
