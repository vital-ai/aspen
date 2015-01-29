/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.domain;

public class AbbreviationInstance extends URIResource {

	private Integer shortFormStart;
	
	private Integer shortFormEnd;
	
	private String shortForm;
	
	private Integer longFormStart;
	
	private Integer longFormEnd;
	
	private String longForm;

	public Integer getShortFormStart() {
		return shortFormStart;
	}

	public void setShortFormStart(Integer shortFormStart) {
		this.shortFormStart = shortFormStart;
	}

	public Integer getShortFormEnd() {
		return shortFormEnd;
	}

	public void setShortFormEnd(Integer shortFormEnd) {
		this.shortFormEnd = shortFormEnd;
	}

	public String getShortForm() {
		return shortForm;
	}

	public void setShortForm(String shortForm) {
		this.shortForm = shortForm;
	}

	public Integer getLongFormStart() {
		return longFormStart;
	}

	public void setLongFormStart(Integer longFormStart) {
		this.longFormStart = longFormStart;
	}

	public Integer getLongFormEnd() {
		return longFormEnd;
	}

	public void setLongFormEnd(Integer longFormEnd) {
		this.longFormEnd = longFormEnd;
	}

	public String getLongForm() {
		return longForm;
	}

	public void setLongForm(String longForm) {
		this.longForm = longForm;
	}
	
}
