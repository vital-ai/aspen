package ai.vital.aspen.groovy.predict.tasks;

import java.util.Map;

import ai.vital.aspen.groovy.featureextraction.CategoricalFeatureData;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;
import ai.vital.predictmodel.CategoricalFeature;

public class CollectCategoricalFeaturesDataTask extends ModelTrainingTask {

	public CategoricalFeature feature;
	
	public CategoricalFeatureData results;
	
	public String datasetName;

	public CollectCategoricalFeaturesDataTask(Map<String, Object> paramsMap, CategoricalFeature f, String datasetName) {
		super(paramsMap);
		this.feature = f;
		this.datasetName = datasetName;
	}
	
	

	@Override
	public void checkDepenedencies() {
		
		if(feature == null) throw new NullPointerException("No feature set");
		
		if(datasetName == null) throw new NullPointerException("No datasetName set");
		
	}



	@Override
	public void onTaskComplete() {
		
		if(results == null) throw new NullPointerException("No categorical feature data: " + feature.getName());
		
	}

}
