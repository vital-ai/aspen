/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.workflow;

import java.util.List;

public class WorkflowConfig {

	private String name;
	
	private List<StepName> steps;

	public WorkflowConfig(String workflowName, List<StepName> steps) {
		super();
		this.name = workflowName;
		this.steps = steps;
	}
	
	public WorkflowConfig() {
		super();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<StepName> getSteps() {
		return steps;
	}
	
	public void setStep(List<StepName> steps) {
		this.steps = steps;
	}
	
	
	public static class StepName  {
		
		private String name;

		public StepName() {
			super();
		}

		public StepName(String name) {
			super();
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
		
	}

	
}
