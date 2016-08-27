package ai.vital.aspen.groovy.select.tasks;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import ai.vital.aspen.groovy.task.AbstractTask;

public class SelectDataSetTask extends AbstractTask {

	public String modelBuilderPath;
	
	public String outputPath;

	//mutually exclusive
	public String queryPath;
	
	//mutually exclusive
	public String urisListPath;

	public Integer limit;
	
	public Integer percent;

	public Integer maxDocs;
	
	public boolean overwrite = false;

	public boolean uriOnlyOutput = false;

	public SelectDataSetTask(Map<String, Object> globalParameters,
			String modelBuilderPath, String outputPath, String queryPath,
			String urisListPath, Integer limit, Integer percent,
			Integer maxDocs, boolean overwrite, boolean uriOnlyOutput) {
		super(globalParameters);
		this.modelBuilderPath = modelBuilderPath;
		this.outputPath = outputPath;
		this.queryPath = queryPath;
		this.urisListPath = urisListPath;
		this.limit = limit;
		this.percent = percent;
		this.maxDocs = maxDocs;
		this.overwrite = overwrite;
		this.uriOnlyOutput = uriOnlyOutput;
	}

	@Override
	public List<String> getRequiredParams() {
		return Collections.emptyList();
	}

	@Override
	public List<String> getOutputParams() {
		return Arrays.asList(outputPath);
	}

	
	
	
}
