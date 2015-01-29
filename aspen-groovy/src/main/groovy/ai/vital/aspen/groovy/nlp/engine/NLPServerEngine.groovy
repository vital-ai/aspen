/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.vitalsigns.global.GlobalHashTable;
import ai.vital.vitalsigns.ontology.VitalCoreOntology;
import ai.vital.workflow.WorkflowEngineConfig;
import ai.vital.workflow.impl.EngineImpl;

import com.hp.hpl.jena.rdf.model.Property;
import com.thoughtworks.xstream.XStream;

public class NLPServerEngine extends EngineImpl<NLPServerConfig> {

	public final static Logger log = LoggerFactory.getLogger(NLPServerEngine.class);
	
	@Override
	protected String[] getBaseStepsPackages() {
		return  [ "ai.vital.flow.server.nlp.workflowstep", "ai.vital.flow.server.nlp.workflowstep_VS"] as String[];
	}

	@Override
	public void start() {

		//enable global
		log.info("Enabling edge index...");
		GlobalHashTable.get().setEdgeIndexEnabled(true);
		
	}
	
//	This static method must be implemented
	public static void initXStream(XStream xs) {
//		xs.alias("test-step", TestStepConfig.class);
		xs.addDefaultImplementation(NLPServerConfig.class, WorkflowEngineConfig.class);
	}

	@Override
	public Property getStatusProperty() {
		return VitalCoreOntology.hasNlpProcessflowStatus;
	}

	@Override
	public String getStatusShortPropertyName() {
		return "nlpProcessflowStatus";
	}

	@Override
	public Property getTimestampProperty() {
		return VitalCoreOntology.hasNlpProcessflowTimestamp;
	}

	@Override
	public String getTimestampShortPropertyName() {
		return "nlpProcessflowTimestamp";
	}

}
