/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.domain;

import java.util.List;



public class Topic extends URIResource {

	private String classifierName;
	
	private String name;
	
	private Float score;
	
	private List<NormalizedTopic> normalizedTopics;

	public String getClassifierName() {
		return classifierName;
	}

	public void setClassifierName(String classifierName) {
		this.classifierName = classifierName;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Float getScore() {
		return score;
	}

	public void setScore(Float score) {
		this.score = score;
	}

	public List<NormalizedTopic> getNormalizedTopics() {
		return normalizedTopics;
	}

	public void setNormalizedTopics(List<NormalizedTopic> normalizedTopics) {
		this.normalizedTopics = normalizedTopics;
	}

}
