/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.workflowstep_VS

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.hp.hpl.jena.rdf.model.Model;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.DefaultExtractor;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import ai.vital.domain.Content;
import ai.vital.domain.Document;
import ai.vital.domain.Edge_hasTextBlock;
import ai.vital.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.boilerpipe.CommentsSectionsFilter;
import ai.vital.aspen.groovy.nlp.utils.TitleExtractor;
import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.html.HTMLParser_VS;
import ai.vital.aspen.groovy.nlp.model.DocumentUtils;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.aspen.groovy.nlp.model.TransformationVectorUtils;
import ai.vital.flow.server.ontology.VitalOntology;
import ai.vital.aspen.groovy.nlp.workflowstep.TextExtractionWorkflowStep;
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.container.Payload;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepV2Impl;

class TextExtractionWorkflowStep_VS extends WorkflowStepV2Impl<NLPServerConfig> {

	public final static StepName TEXTEXTRACTION_VS = new StepName("textextraction_vs");
	
	private final static Logger log = LoggerFactory.getLogger(TextExtractionWorkflowStep.class);
	
	@Override
	public String getName() {
		return TEXTEXTRACTION_VS.getName();
	}

	@Override
	public void processPayload(Payload payload)
			throws ai.vital.workflow.IWorkflowStep.WorkflowHaltException,
			ai.vital.workflow.IWorkflowStep.ProcessflowHaltException,
			Exception {

		for( Document doc : payload.iterator(Document.class) ) {
		
			String uri = doc.getURI();
			
			log.debug("Processing doc {}", uri);
			
			
			String body = null;
			
			
			List<Content> contentsList = doc.getCollection("contents", GraphContext.Container, payload);
			if(contentsList != null && contentsList.size() > 0) {
				Content content = contentsList.get(0);
				body = content.body;
			}
			
			//for backward compatibility read the body from the doc object as well
			if(body == null) {
				body = doc.body;
			}

			if(body == null) {
				log.warn("No body found in document: {}", uri );
				continue;
			}
						
			
			//try parsing the document with jsoup - 
			org.jsoup.nodes.Document document = null;
			
			try {
				if(body.toLowerCase().contains("<html") || body.trim().startsWith("<!DOCTYPE html>")) {
					document = Jsoup.parse(body);
				}
			} catch(Exception e) {
			}
			
			
			List<TextBlock> blocks = new ArrayList<TextBlock>();
			
			String extractedText = null;
			
			if(document == null) {
				
				log.info("The document {} body is not parseable html...", uri);
				
				TextBlock textBlock = new TextBlock();
				textBlock.textBlockLength = body.length();
				textBlock.textBlockOffset = 0;
				textBlock.text = body;
				
				int[] tv = new int[body.length()];
				Arrays.fill(tv, 1);
				TransformationVectorUtils.setTransformationVector(textBlock, tv);
				
				blocks.add(textBlock);
				
				textBlock.setURI(doc.getURI() + "#textBlock_" + blocks.size());
				
				extractedText = body;
				
				if(!doc.title) {
					doc.title = "(untitled)";
				}
				
				
			} else {
			
				log.info("The document {} is HTML - processing with boilerpipe ...", uri);
			
				//also extract title if not available
				if(!doc.title) {
					doc.title = TitleExtractor.extractTitle(body);
				}
				
		    	TextDocument textDoc = new BoilerpipeSAXInput(new InputSource(
		                new StringReader(body))).getTextDocument();
				
				DefaultExtractor.getInstance().process(textDoc);
				
				CommentsSectionsFilter.getInstance().process(textDoc);
				
				List<de.l3s.boilerpipe.document.TextBlock> textBlocks = textDoc.getTextBlocks();
				
				log.info("Extracted {}" + textBlocks);
				
				for(de.l3s.boilerpipe.document.TextBlock tb : textBlocks) {
					
					if(tb.isContent()) {
				
						log.info("Adding content blocks #" + ( blocks.size() + 1) );
								
						TextBlock b = new TextBlock();
						
						//we need to set the text manually and create the conversion vector
						b.textBlockLength = tb.getCharOffsetEnd() - tb.getCharOffsetStart();
						b.textBlockOffset = tb.getCharOffsetStart();
						b.text = tb.getText();
						
						blocks.add(b);
						b.setURI(doc.getURI() + "#textBlock_" + blocks.size());
						
					}
					
				}
				
				log.info("Muting HTML tags...");
				HTMLParser_VS.muteHTML(payload, blocks, body);

				extractedText = textDoc.getContent();
//				extractedText = DocumentUtils.getTextBlocksContent(doc);
				
			}
			
			log.info("Saving step results...");
			
			payload.putGraphObjects(blocks);
			
			payload.putGraphObjects(EdgeUtils.createEdges(doc, blocks, Edge_hasTextBlock.class, VitalOntology.Edge_hasTextBlockURIBase));
			
			doc.extractedText = extractedText;
		
		}
		
	}

}
