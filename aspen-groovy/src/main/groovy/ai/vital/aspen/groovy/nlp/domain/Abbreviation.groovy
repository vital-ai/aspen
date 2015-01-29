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

public class Abbreviation extends URIResource {

	private String shortForm;
	
	private String longForm;
	
	private List<AbbreviationInstance> abbreviationInstances = new ArrayList<AbbreviationInstance>();

	public String getShortForm() {
		return shortForm;
	}

	public void setShortForm(String shortForm) {
		this.shortForm = shortForm;
	}

	public String getLongForm() {
		return longForm;
	}

	public void setLongForm(String longForm) {
		this.longForm = longForm;
	}

	public List<AbbreviationInstance> getAbbreviationInstances() {
		return abbreviationInstances;
	}

	public void setAbbreviationInstances(
			List<AbbreviationInstance> abbreviationInstances) {
		this.abbreviationInstances = abbreviationInstances;
	}
	
}
