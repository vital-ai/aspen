package ai.vital.aspen.groovy.predict.tasks;

import java.util.Map;

import ai.vital.aspen.groovy.featureextraction.NumericalFeatureData;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;
import ai.vital.predictmodel.NumericalFeature;

public class CollectNumericalFeatureDataTask extends ModelTrainingTask {

	public NumericalFeature feature;
	
	public String datasetName;
	
	public NumericalFeatureData results;

	
	public CollectNumericalFeatureDataTask(Map<String, Object> paramsMap, NumericalFeature f, String datasetName) {
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
		
		if(results == null) throw new NullPointerException("No numerical feature data: " + feature.getName());
		
	}

}
