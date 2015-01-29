/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.domain;

public class EntityInstance extends URIResource {

	//offset relative to the body
	private Integer offset;
	
	//length relative to body
	private Integer length;
	
	//offset relative to sentence
	private Integer offsetInSentence;
	
	//length relative to sentence, decoded 
	private Integer lengthInSentence;

	private String exactString;
	
	
	public Integer getOffsetInSentence() {
		return offsetInSentence;
	}

	public void setOffsetInSentence(Integer offsetInSentence) {
		this.offsetInSentence = offsetInSentence;
	}

	public Integer getLengthInSentence() {
		return lengthInSentence;
	}

	public void setLengthInSentence(Integer lengthInSentence) {
		this.lengthInSentence = lengthInSentence;
	}

	public Integer getOffset() {
		return offset;
	}

	public void setOffset(Integer offset) {
		this.offset = offset;
	}

	public Integer getLength() {
		return length;
	}

	public void setLength(Integer length) {
		this.length = length;
	}

	public String getExactString() {
		return exactString;
	}

	public void setExactString(String exactString) {
		this.exactString = exactString;
	}


}
