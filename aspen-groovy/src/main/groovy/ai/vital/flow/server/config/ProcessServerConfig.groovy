/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.flow.server.config;


import ai.vital.workflow.WorkflowEngineConfig;


public class ProcessServerConfig {

	private String id;
	
	private HttpConnector httpConnector;

	private LoggerConfig logger;
	
	private JMSComponentConfig jms;
	
	private DataserverConfig dataserver;

	
	
	private WorkflowEngineConfig engine;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public HttpConnector getHttpConnector() {
		return httpConnector;
	}

	public void setHttpConnector(HttpConnector httpConnector) {
		this.httpConnector = httpConnector;
	}
	
	public LoggerConfig getLogger() {
		return logger;
	}

	public void setLogger(LoggerConfig logger) {
		this.logger = logger;
	}

	public JMSComponentConfig getJms() {
		return jms;
	}

	public void setJms(JMSComponentConfig jms) {
		this.jms = jms;
	}
	
	public DataserverConfig getDataserver() {
		return dataserver;
	}

	public void setDataserver(DataserverConfig dataserver) {
		this.dataserver = dataserver;
	}

	

	

	

	public WorkflowEngineConfig getEngine() {
		return engine;
	}

	public void setEngine(WorkflowEngineConfig engine) {
		this.engine = engine;
	}

}
