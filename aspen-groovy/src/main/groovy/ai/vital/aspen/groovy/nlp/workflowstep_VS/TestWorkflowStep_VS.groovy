/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.workflowstep_VS

import org.slf4j.Logger
import org.slf4j.LoggerFactory;

import ai.vital.aspen.groovy.nlp.config.NLPServerConfig
import ai.vital.vitalsigns.model.container.Payload;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepV2Impl;

class TestWorkflowStep_VS extends WorkflowStepV2Impl<NLPServerConfig> {

	private final static Logger log = LoggerFactory.getLogger(TestWorkflowStep_VS.class);
	
	public final static StepName TEST_VS = new StepName("test_vs");
	
	@Override
	public String getName() {
		return TEST_VS.getName();
	}
	
	@Override
	public void processPayload(Payload payload)
			throws ai.vital.workflow.IWorkflowStep.WorkflowHaltException,
			ai.vital.workflow.IWorkflowStep.ProcessflowHaltException,
			Exception {
		
		log.info("TEST STEP - DO NOTHING");
		
	}

}
