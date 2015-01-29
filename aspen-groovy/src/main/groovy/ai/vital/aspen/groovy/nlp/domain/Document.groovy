/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.domain

import java.util.ArrayList
import java.util.List


public class Document extends URIResource {

	private String title;
	
	private String body;
	
	private String url;
	
	//extracted text
	private String extractedText;
	
	private List<Entity> entities = new ArrayList<Entity>();
	
	private List<Topic> topics = new ArrayList<Topic>();

	private List<TextBlock> textBlocks = new ArrayList<TextBlock>();
	
	private List<Abbreviation> abbreviations = new ArrayList<Abbreviation>();
	
	private List<Annotation> annotations = new ArrayList<Annotation>();
	
	private List<Category> categories = new ArrayList<Category>();
	
	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public List<Entity> getEntities() {
		return entities;
	}

	public void setEntities(List<Entity> entities) {
		this.entities = entities;
	}

	public List<Topic> getTopics() {
		return topics;
	}

	public void setTopics(List<Topic> topics) {
		this.topics = topics;
	}

	public String getExtractedText() {
		return extractedText;
	}

	public void setExtractedText(String extractedText) {
		this.extractedText = extractedText;
	}

	public List<TextBlock> getTextBlocks() {
		return textBlocks;
	}

	public void setTextBlocks(List<TextBlock> textBlocks) {
		this.textBlocks = textBlocks;
	}

	public List<Abbreviation> getAbbreviations() {
		return abbreviations;
	}

	public void setAbbreviations(List<Abbreviation> abbreviations) {
		this.abbreviations = abbreviations;
	}
	
	public List<Annotation> getAnnotations() {
		return annotations;
	}

	public void setAnnotations(List<Annotation> annotations) {
		this.annotations = annotations;
	}

	public List<Category> getCategories() {
		return categories;
	}

	public void setCategories(List<Category> categories) {
		this.categories = categories;
	}

	public String getTextBlocksContent() {

		StringBuilder sb = new StringBuilder();
		
		boolean first = true;
		
		for(TextBlock b : textBlocks) {
			
			if(first) {
				first = false;
			} else {
				sb.append(' ');
			}
			
			sb.append(b.getText());
			
			
		}
		
		return sb.toString();
		
	}

	public TextBlock getTextBlockForOffset(int offset) {
	
		//first we need to identify the block it is located in
		
		int cursor = 0;
		
		int offsetWithinBlock = -1;
		
		for(TextBlock tb : textBlocks) {
			
			String text = tb.getText();
			
			int length = text.length();
			
			if(offset >= cursor && offset <= cursor + length) {
				
				offsetWithinBlock = offset - cursor;
				
				return tb;
				
			}

			//space!
			cursor = cursor + length + 1;
			
		}
		
		return null;
		
	}
	
	public Integer translateBlocksContentOffsetToBodyOffset(int offset) {

		
		//first we need to identify the block it is located in
		
		TextBlock b = null; 
		
		int cursor = 0;
		
		int offsetWithinBlock = -1;
		
		for(TextBlock tb : textBlocks) {
			
			String text = tb.getText();
			
			int length = text.length();
			
			if(offset >= cursor && offset <= cursor + length) {
				
				offsetWithinBlock = offset - cursor;
				
				b = tb;
				
				break;
				
			}

			//space!
			cursor = cursor + length + 1;
			
		}
		
		if(b != null) {
			
			//now translate the block offset and also include the

			int accumulator = 0;
			
			int[] transformationVector = b.getTransformationVector();
			
			int transformationVectorCursor = 0;
			
			
			for(int i = 0; i < offsetWithinBlock; i++) {

				int lastReadValue = transformationVector[transformationVectorCursor++];
				
				accumulator++;
				
				while(lastReadValue == 0) {
					
					lastReadValue = transformationVector[transformationVectorCursor++];
					
					accumulator++;
					
				}
				
			}
			
			return b.getOffset() + accumulator;
			
		}
		
		return null;
	}

	//return sentence, offset within sentence
	public Object[] translateBlocksContentOffsetToSentenceOffset(int offset) {
		
		//first we need to identify the block it is located in
		
		int cursor = 0;
		
		int offsetWithinBlock = -1;
		
		int offsetWithinSentence = -1;
		
		Sentence sentence = null;
		
		for(TextBlock tb : textBlocks) {
			
			String text = tb.getText();
			
			int length = text.length();
			
			if(offset >= cursor && offset <= cursor + length) {
				
				offsetWithinBlock = offset - cursor;
				
				//localize the sentence
				for(Sentence s : tb.getSentences()) {
					
					int start = cursor + s.getStart();
					int end = cursor + s.getEnd();
					
					if(offset >= start && offset <= end) {
						
						sentence = s;
						offsetWithinSentence = offset - start;
						break;
						
					}
					
				}
				
				break;
				
			}

			//space!
			cursor = cursor + length + 1;
			
		}
		
		return [sentence, offsetWithinSentence] as Object[]
		
	}


}
