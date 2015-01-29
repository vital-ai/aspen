/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.domain;

import java.util.ArrayList;
import java.util.List;



public class TextBlock extends URIResource {

	//block offset (relative to doc body)
	private Integer offset;
	
	//block length (relative to doc body), not always length == text.length()
	private Integer length;
	
	//decoded text
	private String text;
	
	private List<Sentence> sentences = new ArrayList<Sentence>();
	
	private int[] transformationVector;
	
	private List<TagElement> tags = new ArrayList<TagElement>();

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

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public List<Sentence> getSentences() {
		return sentences;
	}

	public void setSentences(List<Sentence> sentences) {
		this.sentences = sentences;
	}

	public List<TagElement> getTags() {
		return tags;
	}

	public void setTags(List<TagElement> tags) {
		this.tags = tags;
	}

	public int[] getTransformationVector() {
		return transformationVector;
	}

	public void setTransformationVector(int[] transformationVector) {
		this.transformationVector = transformationVector;
	}

}
