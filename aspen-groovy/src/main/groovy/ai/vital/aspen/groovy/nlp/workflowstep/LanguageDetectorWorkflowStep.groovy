/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.workflowstep;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.knallgrau.utils.textcat.TextCategorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;

import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.domain.Annotation;
import ai.vital.aspen.groovy.nlp.domain.Document;
import ai.vital.aspen.groovy.nlp.domain.rdf.DocumentExtractor;
import ai.vital.aspen.groovy.nlp.models.LanguageDetector;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepImpl;

public class LanguageDetectorWorkflowStep extends
		WorkflowStepImpl<NLPServerConfig> {

	private final static Logger log = LoggerFactory.getLogger(LanguageDetectorWorkflowStep.class);
	
	public final static StepName LANGUAGEDETECTOR = new StepName("languagedetector");
	
	private TextCategorizer languageDetector;
	
	@Override
	public String getName() {
		return LANGUAGEDETECTOR.getName();
	}
	
	@Override
	public void init(NLPServerConfig config) throws StepInitializationException {
		super.init(config);
		
		log.info("Initializing Language Detector...");
		
		LanguageDetector.init();
		
		languageDetector = LanguageDetector.getLanguageDetector();
		
	}
	

	@Override
	public Model processModel(Model model, Map<String, Serializable> context)
			throws ai.vital.workflow.IWorkflowStep.WorkflowHaltException,
			ai.vital.workflow.IWorkflowStep.ProcessflowHaltException,
			Exception {
		
		List<Document> docs = DocumentExtractor.readDocuments(model);
		
		for( Document doc : docs ) {
			
			String docUri = doc.getUri();
			
			log.info("Processing document {} ...", docUri);
			
			String text = doc.getTextBlocksContent();
			
			String lang = languageDetector.categorize(text);
			
			String langID = LanguageDetector.toLanguageID(lang);
			
			log.info("Detected language: {}, id: {}", lang, langID);
			
			Annotation a = new Annotation();
			a.setAnnotationName("language-id");
			a.setAnnotationValue(langID);
			a.setUri(docUri + "#languageAnnotation");
			
			doc.getAnnotations().add(a);
			
			DocumentExtractor.updateDoc(model, doc);
			
		}
		
		return model;
		
	}

}
