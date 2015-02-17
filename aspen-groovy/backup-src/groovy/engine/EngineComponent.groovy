/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.flow.server.engine;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.domain.Document;
import ai.vital.flow.server.ServerComponent;
import ai.vital.flow.server.ServerComponentException;
import ai.vital.flow.server.config.ProcessServerConfig;
import ai.vital.flow.server.config.ProcessServerConfigManager;
import ai.vital.vitalsigns.global.GlobalHashTable;
import ai.vital.vitalsigns.model.PropertyImplementation;
import ai.vital.vitalsigns.model.container.Message;
import ai.vital.vitalsigns.model.container.Payload;
import ai.vital.vitalsigns.model.container.ProcessFlowStep;
import ai.vital.workflow.IEngine;
import ai.vital.workflow.WorkflowEngineConfig;
import ai.vital.workflow.IWorkflowStep.ProcessflowHaltException;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;

/**
 * The wrapper for a workflow engine. Contains the core of each processing step - the engine capable of modifying input model.
 * Similarly to the workflow eingine, the engine component is free of any messaging abstraction.
 * 
 * IMPORTANT an instance of IEngine must also define the <code>public static void initXStream(XStream xs)</code> method which
 * will be used for xml <-> config object mapping. See {@link ProcessServerConfigManager#loadConfigFromFile(String, Class)} 
 * for details.
 * @author Derek
 *
 */
public class EngineComponent implements ServerComponent<WorkflowEngineConfig> {

	private final static Logger log = LoggerFactory.getLogger(EngineComponent.class);
	
	//current message is used just for the steps that need access to it, when multithreading is added consider using some hashmap 
	private Message currentMessage = null;
	
	protected WorkflowEngineConfig configObject;
	
//	This static method must be implemented in the IEngine
//	public static void initXStream(XStream xs) {}
	protected IEngine engineImpl; 
	
	public EngineComponent(IEngine engineImpl) {
		super();
		this.engineImpl = engineImpl;
	}

	public void init(WorkflowEngineConfig configObject, ProcessServerConfig mainConfig) throws ServerComponentException {
		
		if(configObject instanceof AGraphConfigObject) {
			
			((AGraphConfigObject)configObject).setAgraph(ThriveProcessServer.get().config.getAgraph());
			
		}
		
		if(configObject instanceof _4StoreConfigObject) {
			
			((_4StoreConfigObject)configObject).set_4Store(ThriveProcessServer.get().config.get_4store());
			
		}
		
		if(configObject instanceof DataserverConfigObject) {
		
			((DataserverConfigObject)configObject).setDataserver(ThriveProcessServer.get().config.getDataserver());
			
		}
		
		try {
			engineImpl.initEngine(configObject);
		} catch (Exception e) {
			throw new ServerComponentException(e);
		}
		
		this.configObject = configObject;
		
	};
	
	/**
	 * Processes given input model. The output model is the only result. Technically it may be same object. 
	 * @param model
	 * @param context 
	 * @return processed model
	 * @throws Exception
	 */
	public Model process(String workflowName, Model model, Map<String, Serializable> context) throws Exception, ProcessflowHaltException {
		return engineImpl.processModel(workflowName, model, context);
	}
	
	public void process(String workflowName, Payload payload, ProcessFlowStep step) throws Exception, ProcessflowHaltException {
		try {
			engineImpl.processPayload(workflowName, payload, step);
		}catch(ProcessflowHaltException e) {
			throw e;
		}catch(Exception e) {
			
			for(Iterator<Document> iter = payload.iterator(Document.class); iter.hasNext(); ) {
				
				Document doc = iter.next();
				
				String statusShortProperty = engineImpl.getStatusShortPropertyName();
				Property statusProperty = engineImpl.getStatusProperty();
				PropertyImplementation p1 = new PropertyImplementation(statusShortProperty, statusProperty.getURI(), String.class);
				p1.setValue("OK");
				
				String timestampShortProperty = engineImpl.getTimestampShortPropertyName();
				Property timestampProperty = engineImpl.getTimestampProperty();
				
				PropertyImplementation p2 = new PropertyImplementation(timestampShortProperty, timestampProperty.getURI(), Long.class);
				p2.setValue(System.currentTimeMillis());
				
				doc.getProperties().put(p1.getShortName(), p1);
				doc.getProperties().put(p2.getShortName(), p2);
			}
			
			
			
			throw e;
		} finally {
			if(Boolean.TRUE.equals( configObject.getGlobalHashtableEnabled())) {
				GlobalHashTable.get().purge();
			}
		}
	}
	
	@Override
	public String getName() {
		return "Engine Component";
	}

	@Override
	public void start() throws ServerComponentException {
		log.info("Starting engine: " + engineImpl.getClass().getCanonicalName());
		engineImpl.start();
	}

	@Override
	public void stop() throws ServerComponentException {
		log.info("Stopping engine: " + engineImpl.getClass().getCanonicalName());
//		engineImpl.stop();
		engineImpl.shutdown();
	}

	public Message getCurrentMessage() {
		return currentMessage;
	}

	public void setCurrentMessage(Message currentMessage) {
		this.currentMessage = currentMessage;
	}
	
	
}
