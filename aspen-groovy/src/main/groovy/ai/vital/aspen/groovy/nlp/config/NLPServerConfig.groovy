/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.config;

import ai.vital.flow.server.config.DataserverConfig;
import ai.vital.flow.server.engine.DataserverConfigObject;
import ai.vital.workflow.WorkflowEngineConfig;

public class NLPServerConfig extends WorkflowEngineConfig implements
		DataserverConfigObject {

	private OpenCalaisConfig openCalais;

	private OpenNLPConfig openNLP;

	private MinorthirdConfig minorthird;

	private DataserverConfig dataserver;

	private String entitiesSegment;

	private String topicsSegment;

	public OpenCalaisConfig getOpenCalais() {
		return openCalais;
	}

	public void setOpenCalais(OpenCalaisConfig openCalais) {
		this.openCalais = openCalais;
	}

	public OpenNLPConfig getOpenNLP() {
		return this.openNLP;
	}

	public void setOpenNLP(OpenNLPConfig openNLP) {
		this.openNLP = openNLP;
	}

	public MinorthirdConfig getMinorthird() {
		return minorthird;
	}

	public void setMinorthird(MinorthirdConfig minorthird) {
		this.minorthird = minorthird;
	}

	@Override
	public DataserverConfig getDataserver() {
		return dataserver;
	}

	@Override
	public void setDataserver(DataserverConfig config) {
		this.dataserver = config;
	}

	public String getEntitiesSegment() {
		return entitiesSegment;
	}

	public void setEntitiesSegment(String entitiesSegment) {
		this.entitiesSegment = entitiesSegment;
	}

	public String getTopicsSegment() {
		return topicsSegment;
	}

	public void setTopicsSegment(String topicsSegment) {
		this.topicsSegment = topicsSegment;
	}

}
