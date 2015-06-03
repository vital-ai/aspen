package ai.vital.aspen.groovy.predict.tasks;

import java.util.Map;

import ai.vital.aspen.groovy.featureextraction.TextFeatureData;
import ai.vital.aspen.groovy.modelmanager.AspenModel;
import ai.vital.aspen.groovy.predict.ModelTrainingTask;
import ai.vital.predictmodel.TextFeature;

public class CollectTextFeatureDataTask extends ModelTrainingTask {

	public TextFeature feature;
	
	public String datasetName;

	
	public Integer minDF;
	
	public Integer maxDF;
	
	
	public TextFeatureData results;

	private AspenModel model;
	

	public CollectTextFeatureDataTask(AspenModel model, Map<String, Object> paramsMap, TextFeature f, String datasetName) {
		super(paramsMap);
		this.model = model;
		this.feature = f;
		this.datasetName = datasetName;
	}
	

	@Override
	public void checkDepenedencies() {
		if(feature == null) throw new NullPointerException("No feature set");
		
		if(datasetName == null) throw new NullPointerException("No datasetName set");

		minDF = (Integer) paramsMap.get(datasetName + ProvideMinDFMaxDF.MIN_DF_SUFFIX);
		
		maxDF = (Integer) paramsMap.get(datasetName + ProvideMinDFMaxDF.MAX_DF_SUFFIX);
		
		if(minDF == null) throw new NullPointerException("No minDF set");
		
		if(maxDF == null) throw new NullPointerException("No maxDF set");
		
	}



	@Override
	public void onTaskComplete() {
		
		if(results == null) throw new NullPointerException("No text feature data: " + feature.getName());
		
        model.getFeaturesData().put(feature.getName(), results);
	}
	
	
}
