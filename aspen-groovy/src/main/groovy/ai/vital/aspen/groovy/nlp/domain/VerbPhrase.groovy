/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.domain;

public class VerbPhrase extends URIResource {

	
	//inclusive, relative to sentence
	private Integer startTokenIndex; 
	
	//inclusive, relative to sentence
	private Integer endTokenIndex;

	public Integer getStartTokenIndex() {
		return startTokenIndex;
	}

	public void setStartTokenIndex(Integer startTokenIndex) {
		this.startTokenIndex = startTokenIndex;
	}

	public Integer getEndTokenIndex() {
		return endTokenIndex;
	}

	public void setEndTokenIndex(Integer endTokenIndex) {
		this.endTokenIndex = endTokenIndex;
	}

}
