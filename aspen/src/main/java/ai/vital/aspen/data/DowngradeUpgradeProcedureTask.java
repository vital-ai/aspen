package ai.vital.aspen.data;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.task.AbstractTask;
import ai.vital.vitalservice.ServiceOperations;

public class DowngradeUpgradeProcedureTask extends AbstractTask {

	public final static String DATASET_STATS = "DowngradeUpgradeProcedure_DATASET_STATS"; 
	
	private String inputDatasetName;
	private String outputDatasetName;
	private ServiceOperations ops;

	private String serviceOpsContents;
	
	public DowngradeUpgradeProcedureTask(Map<String, Object> globalParameters, String inputDatasetName, String outputDatasetName, ServiceOperations ops, String serviceOpsContents) {
		super(globalParameters);
		this.inputDatasetName = inputDatasetName;
		this.outputDatasetName = outputDatasetName;
		this.ops = ops;
		this.serviceOpsContents = serviceOpsContents;
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(outputDatasetName, DATASET_STATS);
	}

	@Override
	public List<String> getRequiredParams() {
		return Arrays.asList(inputDatasetName);
	}

	public String getInputDatasetName() {
		return inputDatasetName;
	}

	public void setInputDatasetName(String inputDatasetName) {
		this.inputDatasetName = inputDatasetName;
	}

	public String getOutputDatasetName() {
		return outputDatasetName;
	}

	public void setOutputDatasetName(String outputDatasetName) {
		this.outputDatasetName = outputDatasetName;
	}

	public ServiceOperations getOps() {
		return ops;
	}

	public void setOps(ServiceOperations ops) {
		this.ops = ops;
	}

	public String getServiceOpsContents() {
		return serviceOpsContents;
	}

	public void setServiceOpsContents(String serviceOpsContents) {
		this.serviceOpsContents = serviceOpsContents;
	}
	

}
