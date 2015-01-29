/*******************************************************************************
 * Copyright 2012 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.workflow;

import ai.vital.vitalsigns.model.container.Payload;
import ai.vital.workflow.IWorkflowStep.ProcessflowHaltException
import ai.vital.workflow.IWorkflowStep.WorkflowHaltException

public interface IWorkflowStepV2<T extends WorkflowEngineConfig> extends IWorkflowStep<T> {

	/**
	 * Each workflow step operates on jena RDF model. The input model is mutable thus usually the returned object
	 * is the modified input instance.
	 * @param model
	 * @param context
	 * @return
	 * @throws WorkflowHaltException when workflow step halts the workflow
	 * @throws ProcessflowHaltException when workflow step halts the whole processflow
	 * @throws Exception on error
	 */
	public void processPayload(Payload payload) throws WorkflowHaltException, ProcessflowHaltException, Exception;
	
}
