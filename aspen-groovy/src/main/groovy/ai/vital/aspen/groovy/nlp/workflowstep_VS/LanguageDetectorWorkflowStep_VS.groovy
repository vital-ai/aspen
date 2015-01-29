/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.workflowstep_VS

import org.knallgrau.utils.textcat.TextCategorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.domain.Document
import ai.vital.domain.Annotation;
import ai.vital.domain.Edge_hasAnnotation;
import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.model.DocumentUtils;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.aspen.groovy.nlp.models.LanguageDetector;
import ai.vital.flow.server.ontology.VitalOntology;
import ai.vital.aspen.groovy.nlp.workflowstep.LanguageDetectorWorkflowStep;
import ai.vital.vitalsigns.model.container.Payload;
import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepV2Impl;

class LanguageDetectorWorkflowStep_VS extends WorkflowStepV2Impl<NLPServerConfig> {

	private final static Logger log = LoggerFactory.getLogger(LanguageDetectorWorkflowStep_VS.class);
	
	public final static StepName LANGUAGEDETECTOR_VS = new StepName("languagedetector_vs");
	
	private TextCategorizer languageDetector;
	
	@Override
	public String getName() {
		return LANGUAGEDETECTOR_VS.getName();
	}
	
	@Override
	public void init(NLPServerConfig config) throws StepInitializationException {
		super.init(config);
		
		log.info("Initializing Language Detector...");
		
		LanguageDetector.init();
		
		languageDetector = LanguageDetector.getLanguageDetector();
		
	}
	
	@Override
	public void processPayload(Payload payload)
			throws ai.vital.workflow.IWorkflowStep.WorkflowHaltException,
			ai.vital.workflow.IWorkflowStep.ProcessflowHaltException,
			Exception {

		for( Document doc : payload.iterator(Document.class) ) {
				
			String docUri = doc.getURI();
						
			log.info("Processing document {} ...", docUri);
			
			String text = DocumentUtils.getTextBlocksContent(doc);
			
			String lang = languageDetector.categorize(text);
			
			String langID = LanguageDetector.toLanguageID(lang);
			
			log.info("Detected language: {}, id: {}", lang, langID);
			
			Annotation a = new Annotation();
			a.annotationName = 'language-id';
			a.annotationValue = langID;
			a.URI = docUri + "#languageAnnotation";
			
			payload.putGraphObjects(Arrays.asList(a));
			payload.putGraphObjects(EdgeUtils.createEdges(doc, Arrays.asList(a), Edge_hasAnnotation, VitalOntology.Edge_hasAnnotationURIBase));
			
		}
		
	}


}
