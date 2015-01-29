/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.workflow;

import java.util.ArrayList;
import java.util.List;

/**
 * Workflow is a collection of workflow steps, the steps are run one by one by the engine
 * @author Derek
 *
 */
public class Workflow {

	@SuppressWarnings("rawtypes")
	private List<IWorkflowStep> steps = new ArrayList<IWorkflowStep>();
	
	private String name;

	@SuppressWarnings("rawtypes")
	public List<IWorkflowStep> getSteps() {
		return steps;
	}

	@SuppressWarnings("rawtypes")
	public void setSteps(List<IWorkflowStep> steps) {
		this.steps = steps;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
}
