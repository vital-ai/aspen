/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.workflow;

import java.io.Serializable;
import java.util.Map;

import ai.vital.vitalsigns.model.container.Payload;
import ai.vital.vitalsigns.model.container.ProcessFlowStep;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;

/**
 * An engine is a single processing unit. It's a container of workflows.
 * @author Derek
 * 
 */
public interface IEngine<T extends WorkflowEngineConfig> {
	
	/**
	 * 
	 * @param workflow
	 * @param inputModel
	 * @param context
	 * @return
	 * @throws Exception
	 */
	@Deprecated
	public Model processModel(String workflow, Model inputModel, Map<String, Serializable> context) throws Exception;

	public void processPayload(String workflow, Payload payload, ProcessFlowStep step) throws Exception;
	
	public void start();
	
//	public void stop();

	public void initEngine(T configObject) throws Exception;
	
	public void shutdown();
	
	
	public Property getStatusProperty();
	public String getStatusShortPropertyName();
	
	public Property getTimestampProperty();
	public String getTimestampShortPropertyName();
}
