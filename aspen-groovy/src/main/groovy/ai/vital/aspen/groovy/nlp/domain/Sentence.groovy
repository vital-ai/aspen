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



public class Sentence extends URIResource {

	private Integer start;
	
	private Integer end;

	private Integer sentenceNumber;
	
	private List<Token> tokens = new ArrayList<Token>();

	private List<PosTag> posTags = new ArrayList<PosTag>();

	private List<NounPhrase> nounPhrases = new ArrayList<NounPhrase>();

	private List<VerbPhrase> verbPhrases = new ArrayList<VerbPhrase>();
	
	private List<EntityInstance> entityInstances = new ArrayList<EntityInstance>();
	
	private List<AbbreviationInstance> abbreviationInstances = new ArrayList<AbbreviationInstance>();
	
	public Sentence() {
		super();
	}

	public List<NounPhrase> getNounPhrases() {
		return nounPhrases;
	}

	public void setNounPhrases(List<NounPhrase> nounPhrases) {
		this.nounPhrases = nounPhrases;
	}

	public List<VerbPhrase> getVerbPhrases() {
		return verbPhrases;
	}

	public void setVerbPhrases(List<VerbPhrase> verbPhrases) {
		this.verbPhrases = verbPhrases;
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

	public Integer getSentenceNumber() {
		return sentenceNumber;
	}

	public void setSentenceNumber(Integer sentenceNumber) {
		this.sentenceNumber = sentenceNumber;
	}

	public List<Token> getTokens() {
		return tokens;
	}

	public void setTokens(List<Token> tokens) {
		this.tokens = tokens;
	}

	public List<PosTag> getPosTags() {
		return posTags;
	}

	public void setPosTags(List<PosTag> posTags) {
		this.posTags = posTags;
	}

	public List<EntityInstance> getEntityInstances() {
		return entityInstances;
	}

	public void setEntityInstances(List<EntityInstance> entityInstances) {
		this.entityInstances = entityInstances;
	}

	public List<AbbreviationInstance> getAbbreviationInstances() {
		return abbreviationInstances;
	}

	public void setAbbreviationInstances(
			List<AbbreviationInstance> abbreviationInstances) {
		this.abbreviationInstances = abbreviationInstances;
	}

}
