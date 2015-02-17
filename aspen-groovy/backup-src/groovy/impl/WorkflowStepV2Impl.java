/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.workflow.impl;

import java.io.Serializable;
import java.util.Map;

import com.hp.hpl.jena.rdf.model.Model;

import ai.vital.workflow.IWorkflowStepV2;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowEngineConfig;

public abstract class WorkflowStepV2Impl<T extends WorkflowEngineConfig> implements IWorkflowStepV2<T> {

	protected T config;
	
	@Override
	public void init(T config) throws StepInitializationException {
		this.config = config;
	}
	
	@Override
	public void shutdown() {
		//default implementation does nothing - nothing to clean up
	}

	@Override
	public T getConfig() {
		return config;
	}

	protected long time() {return System.currentTimeMillis();}

	@Override
	public Model processModel(Model model, Map<String, Serializable> context)
			throws ai.vital.workflow.IWorkflowStep.WorkflowHaltException,
			ai.vital.workflow.IWorkflowStep.ProcessflowHaltException,
			Exception {
		throw new NoSuchMethodException("This method is unsupported for workflow step that implements: " + IWorkflowStepV2.class.getCanonicalName());
	}
	
}
