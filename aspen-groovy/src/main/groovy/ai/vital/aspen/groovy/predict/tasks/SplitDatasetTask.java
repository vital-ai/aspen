package ai.vital.aspen.groovy.predict.tasks;

import java.util.Map;

import ai.vital.aspen.groovy.predict.ModelTrainingTask;

public class SplitDatasetTask extends ModelTrainingTask {

	public String inputDatasetName;
	
	public String outputDatasetName1;
	
	public String outputDatasetName2;
	
	public Double firstSplitRatio;
	
	
	public SplitDatasetTask(Map<String, Object> paramsMap, String inputDatasetName, String outputDatasetName1,
			String outputDatasetName2, Double firstSplitRatio) {
		super(paramsMap);
		this.inputDatasetName = inputDatasetName;
		this.outputDatasetName1 = outputDatasetName1;
		this.outputDatasetName2 = outputDatasetName2;
		this.firstSplitRatio = firstSplitRatio;
	}

	

	@Override
	public void checkDepenedencies() {

		if(firstSplitRatio == null) throw new RuntimeException("No firstSplitRatio");
		
		if(firstSplitRatio <= 0d || firstSplitRatio >= 1d) throw new RuntimeException("firstSplitRatio must be in range (0, 1) (exclusive)");
		
		if(inputDatasetName == null) throw new RuntimeException("No inputDatasetName");
		
		if(outputDatasetName1 == null) throw new RuntimeException("No outputDatasetName1");
		
		if(outputDatasetName2 == null) throw new RuntimeException("No outputDatasetName2");
		
	}
	
	@Override
	public void onTaskComplete() {
		
	}

}
