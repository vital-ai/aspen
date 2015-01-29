/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.config;

public class OpenNLPConfig {

	private String modelsDir;

	private String sentencesModel;

	private String englishTokenizerModel;

	private String posTaggerModel;

	private String chunkerModel;

	private String namedPersonModel;

	private String sentimentModel;

	public String getModelsDir() {
		return modelsDir;
	}

	public void setModelsDir(String modelsDir) {
		this.modelsDir = modelsDir;
	}

	public String getSentencesModel() {
		return sentencesModel;
	}

	public void setSentencesModel(String sentencesModel) {
		this.sentencesModel = sentencesModel;
	}

	public String getEnglishTokenizerModel() {
		return englishTokenizerModel;
	}

	public void setEnglishTokenizerModel(String englishTokenizerModel) {
		this.englishTokenizerModel = englishTokenizerModel;
	}

	public String getPosTaggerModel() {
		return posTaggerModel;
	}

	public void setPosTaggerModel(String posTaggerModel) {
		this.posTaggerModel = posTaggerModel;
	}

	public String getChunkerModel() {
		return chunkerModel;
	}

	public void setChunkerModel(String chunkerModel) {
		this.chunkerModel = chunkerModel;
	}

	public String getNamedPersonModel() {
		return namedPersonModel;
	}

	public void setNamedPersonModel(String namedPersonModel) {
		this.namedPersonModel = namedPersonModel;
	}

	public String getSentimentModel() {
		return sentimentModel;
	}

	public void setSentimentModel(String sentimentModel) {
		this.sentimentModel = sentimentModel;
	}
}
