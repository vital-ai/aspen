/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.domain;

import java.util.List;



public class Entity extends URIResource {

	private String name;
	
	private String extractSource;
	
	private String category;
	
	private Float relevance;
	
	private List<EntityInstance> entityInstances;
	
	private List<NormalizedEntity> normalizedEntities;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getExtractSource() {
		return extractSource;
	}

	public void setExtractSource(String extractSource) {
		this.extractSource = extractSource;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public Float getRelevance() {
		return relevance;
	}

	public void setRelevance(Float relevance) {
		this.relevance = relevance;
	}

	public List<EntityInstance> getEntityInstances() {
		return entityInstances;
	}

	public void setEntityInstances(List<EntityInstance> entityInstances) {
		this.entityInstances = entityInstances;
	}

	public List<NormalizedEntity> getNormalizedEntities() {
		return normalizedEntities;
	}

	public void setNormalizedEntities(List<NormalizedEntity> normalizedEntities) {
		this.normalizedEntities = normalizedEntities;
	}

}
