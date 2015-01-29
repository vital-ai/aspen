/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.domain;


public class TagElement extends URIResource {

	private Boolean opening;
	
	private Boolean standalone;
	
	private Boolean closing;
	
	private Integer start;
	
	private Integer end;
	
	private String tag;

	public Boolean getOpening() {
		return opening;
	}

	public void setOpening(Boolean opening) {
		this.opening = opening;
	}

	public Boolean getStandalone() {
		return standalone;
	}

	public void setStandalone(Boolean standalone) {
		this.standalone = standalone;
	}

	public Boolean getClosing() {
		return closing;
	}

	public void setClosing(Boolean closing) {
		this.closing = closing;
	}

	public Integer getStart() {
		return start;
	}

	public void setStart(Integer start) {
		this.start = start;
	}

	public Integer getEnd() {
		return end;
	}

	public void setEnd(Integer end) {
		this.end = end;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}
	
}
