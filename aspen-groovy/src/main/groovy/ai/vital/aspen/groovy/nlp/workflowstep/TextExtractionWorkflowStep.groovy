/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.workflowstep;

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

import ai.vital.aspen.groovy.nlp.boilerpipe.CommentsSectionsFilter;
import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.domain.Document;
import ai.vital.aspen.groovy.nlp.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.domain.rdf.DocumentExtractor;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepImpl;

import com.hp.hpl.jena.rdf.model.Model;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.DefaultExtractor;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;

public class TextExtractionWorkflowStep extends WorkflowStepImpl<NLPServerConfig>{

	public final static StepName TEXTEXTRACTION = new StepName("textextraction");
	
	private final static Logger log = LoggerFactory.getLogger(TextExtractionWorkflowStep.class);
	
	@Override
	public String getName() {
		return TEXTEXTRACTION.getName();
	}

	@Override
	public void init(NLPServerConfig config) throws StepInitializationException {
		super.init(config);
		log.info("Setting up Vital Signs...");
		VitalSigns.get();//.setEdgesResolver(new GlobalHashTableEdgesResolver());
		log.info("Vital signs set up.");
	}
	
	@Override
	public Model processModel(Model model, Map<String, Serializable> context)
			throws ai.vital.workflow.IWorkflowStep.WorkflowHaltException,
			ai.vital.workflow.IWorkflowStep.ProcessflowHaltException,
			Exception {

		List<Document> docs = DocumentExtractor.readDocuments(model);
		
		for( Document doc : docs ) {
		
			String uri = doc.getUri();
			
			log.debug("Processing doc {}", uri);
			
			String body = doc.getBody();
			
			//try parsing the document with jsoup - 
			org.jsoup.nodes.Document document = null;
			
			try {
				
				if(body.toLowerCase().contains("<html")) {
					document = Jsoup.parse(body);
				}
				
			} catch(Exception e) {
			}
			
			
			List<TextBlock> blocks = new ArrayList<TextBlock>();
			
			String extractedText = null;
			
			if(document == null) {
				
				TextBlock textBlock = new TextBlock();
				textBlock.setLength(body.length());
				textBlock.setOffset(0);
				textBlock.setText(body);
				
//				for(int i = 0 ; i < body.length(); i++) {
//					textBlock.transformationVector.add(new Integer(1));
//				}
				int[] tv = new int[body.length()];
				Arrays.fill(tv, 1);
				textBlock.setTransformationVector(tv);
				
				blocks.add(textBlock);
				textBlock.setUri(doc.getUri() + "#textBlock_" + blocks.size());
				
				extractedText = body;
				
				log.debug("The document {} body is not parseable html...", uri);
				
			} else {
				
				Element element = document.getElementById("id");
				
		    	TextDocument textDoc = new BoilerpipeSAXInput(new InputSource(
		                new StringReader(body))).getTextDocument();
				
		    	
//				boolean processed = 
//				ArticleSentencesExtractor.getInstance().process(textDoc);
//				ArticleExtractor.getInstance().process(textDoc);

				
				DefaultExtractor.getInstance().process(textDoc);
				
				CommentsSectionsFilter.getInstance().process(textDoc);
				
				
				List<de.l3s.boilerpipe.document.TextBlock> textBlocks = textDoc.getTextBlocks();
				
				for(de.l3s.boilerpipe.document.TextBlock tb : textBlocks) {
					
					if(tb.isContent()) {
						
						TextBlock b = new TextBlock();
						
						b.setLength(tb.getCharOffsetEnd() - tb.getCharOffsetStart());

						b.setOffset(tb.getCharOffsetStart());
						
						//we need to set the text manually and create the conversion vector
						b.setText(tb.getText());
						
						
						blocks.add(b);
						b.setUri(doc.getUri() + "#textBlock_" + blocks.size());
//						String substring = body.substring(tb.getCharOffsetStart(), tb.getCharOffsetEnd());
						
//						System.out.println(substring);
//						System.out.println(tb.getText());
						
						
					}
					
				}

				HTMLParser.muteHTML(blocks, body);

				StringBuilder sb = new StringBuilder();
				
				boolean first = true;
				
				for(int i = 0 ; i < textBlocks.size(); i++ ) {
					
					de.l3s.boilerpipe.document.TextBlock tb = textBlocks.get(i);
					
					if(first) {
						first = false;
					} else {
						sb.append(' ');
					}
					
					sb.append(tb.getText());
					
					
				}
				
				extractedText = sb.toString();
				
				
//				int pointer = 0;
//				
//				for(int i = 0 ; i < textBlocks.size(); i++ ) {
//					
//					de.l3s.boilerpipe.document.TextBlock tb = textBlocks.get(i);
//					
//					if(tb.isContent()) {
//				
//						
//						TextBlock b = blocks.get(pointer);
//						
//						pointer++;
//						//we need to set the text manually and create the conversion vector
//						
//						
//						String substring = body.substring(tb.getCharOffsetStart(), tb.getCharOffsetEnd());
//						
//						System.out.println("BLOCK " + i + " COMPARISON: ");
//						System.out.println(tb.getText());
//						System.out.println(b.getText());
//						
//						
//					}
//					
//				}
				
			}
			
			doc.setTextBlocks(blocks);
			
//			String extractedTitle = null;
////			String extractedTitle = TitleExctractor.extractTitle(document);
//			
//			
//        	File td = new File("bp3test");
//        	td.mkdir();
//        	
//        	testExtractor(td, ArticleSentencesExtractor.getInstance(), body);
//        	testExtractor(td, ArticleExtractor.getInstance(), body);
//			testExtractor(td, CanolaExtractor.getInstance(), body);
//        	testExtractor(td, DefaultExtractor.getInstance(), body);
//        	testExtractor(td, DefaultExtractor.getInstance(), body);
//        	testExtractor(td, KeepEverythingExtractor.INSTANCE, body);
//        	testExtractor(td, new KeepEverythingWithMinKWordsExtractor(10), body);
//        	testExtractor(td, LargestContentExtractor.getInstance(), body);
//        	testExtractor(td, NumWordsRulesExtractor.getInstance(), body);
//        	
//			log.debug("Extracted the following title from HTML: {}" + extractedTitle);
//			
			doc.setExtractedText(extractedText);
			
//			ModelUtils.addStringLiteral(docR, VitalNLPOnt.hasExtractedTitle, extractedTitle, true);
			
			DocumentExtractor.updateDoc(model, doc);
		}
		
		return model;
	}

	private void testExtractor(File targetDir, de.l3s.boilerpipe.BoilerpipeExtractor instance, String html) throws BoilerpipeProcessingException, IOException, SAXException {

    	TextDocument body = new BoilerpipeSAXInput(new InputSource(
                new StringReader(html))).getTextDocument();
		
		
		boolean process = instance.process(body);
		
		String content = body.getContent();
		
		String title = body.getTitle();
		
		String t1= body.getText(false, false);
		
		String t2= body.getText(true, false);
		
		String t3= body.getText(false, true);
		
		String t4= body.getText(true, true);
		
//		System.out.println(content);
		
//		int isContent = 0;
//		int nonContent = 0;
//		
//		for(TextBlock tb : textBlocks) {
//			if( tb.isContent() ) {
//				isContent++;
//			} else {
//				nonContent++;
//			}
//		}
		
		
		writeFile(new File(targetDir, instance.getClass().getSimpleName() + "_content.txt"), content);
		
		writeFile(new File(targetDir, instance.getClass().getSimpleName() + "_t1.txt"), t1);
		writeFile(new File(targetDir, instance.getClass().getSimpleName() + "_t2.txt"), t2);
		writeFile(new File(targetDir, instance.getClass().getSimpleName() + "_t3.txt"), t3);
		writeFile(new File(targetDir, instance.getClass().getSimpleName() + "_t4.txt"), t4);
		
	}

	private void writeFile(File f, String content) throws IOException {
		
		FileOutputStream fos = new FileOutputStream(f);
		
		fos.write(content.getBytes("UTF-8"));
		
		fos.close();
	}

}
