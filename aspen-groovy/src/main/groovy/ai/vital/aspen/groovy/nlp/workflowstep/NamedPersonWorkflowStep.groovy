/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.workflowstep;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.util.Span;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.config.OpenNLPConfig;
import ai.vital.aspen.groovy.nlp.domain.Document;
import ai.vital.aspen.groovy.nlp.domain.Entity;
import ai.vital.aspen.groovy.nlp.domain.EntityInstance;
import ai.vital.aspen.groovy.nlp.domain.Sentence;
import ai.vital.aspen.groovy.nlp.domain.TextBlock;
import ai.vital.aspen.groovy.nlp.domain.Token;
import ai.vital.aspen.groovy.nlp.domain.rdf.DocumentExtractor;
import ai.vital.aspen.groovy.nlp.models.NamedPersonModel;

// changed from namedperson
import ai.vital.domain.Person;


import ai.vital.workflow.StepInitializationException;
import ai.vital.workflow.WorkflowConfig.StepName;
import ai.vital.workflow.impl.WorkflowStepImpl;

import com.hp.hpl.jena.rdf.model.Model;

public class NamedPersonWorkflowStep extends WorkflowStepImpl<NLPServerConfig> {

	public final static StepName NAMEDPERSONTAGGER = new StepName("namedpersontagger");
	
	private final static Logger log = LoggerFactory.getLogger(NamedPersonWorkflowStep.class);
	
	private NameFinderME nameFinder;
	
	@Override
	public String getName() {
		return NAMEDPERSONTAGGER.getName();
	}
	
	@Override
	public void init(NLPServerConfig config) throws StepInitializationException {
		super.init(config);
		
		OpenNLPConfig openNLP = config.getOpenNLP();
		File namedPersonModelFile = new File(openNLP.getModelsDir(), openNLP.getNamedPersonModel());
		
		log.info("Initializing named person model from file: {} ...", namedPersonModelFile.getAbsolutePath());
		
		NamedPersonModel.init(namedPersonModelFile);
		
		nameFinder = NamedPersonModel.getNameFinder();
		
	}

	@Override
	public Model processModel(Model model, Map<String, Serializable> context)
			throws ai.vital.workflow.IWorkflowStep.WorkflowHaltException,
			ai.vital.workflow.IWorkflowStep.ProcessflowHaltException,
			Exception {

		List<Document> docs = DocumentExtractor.readDocuments(model);
		
		for(Document doc : docs) {
			
			String docUri = doc.getUri();
			
			log.info("Processing document with URI: " + docUri);
			
			List<Entity> namedPersons = new ArrayList<Entity>();
			
			int blockOffset = 0;
			
			for(TextBlock b : doc.getTextBlocks()) {
				
				String blockText = b.getText();
				
				for(Sentence sentence : b.getSentences()) {

					int sentenceOffset = sentence.getStart();
					
					String sentenceText = blockText.substring(sentence.getStart(), sentence.getEnd());
					
					List<Token> tokens = sentence.getTokens();
					
					String[] sentenceA = new String[tokens.size()];
					
					for(int i = 0 ; i < tokens.size(); i++) {
						sentenceA[i] = tokens.get(i).getText();
					}
					
					Span[] names = nameFinder.find(sentenceA);
					
					for(Span n : names) {
						
						Token firstToken = tokens.get(n.getStart());
						Token lastToken = tokens.get(n.getEnd() -1 );

						Entity np = new Entity();
						np.setUri(docUri + "#Person_" + namedPersons.size());
						
						String substring = sentenceText.substring(firstToken.getStart(), lastToken.getEnd());
						np.setName(substring);
						np.setExtractSource("OpenNLP");
						np.setCategory(NamedPerson.class.getSimpleName());

						EntityInstance entityInstance = new EntityInstance();
						entityInstance.setUri(np.getUri() + "instance_0");
						entityInstance.setExactString(substring);
						entityInstance.setLengthInSentence(substring.length());
						entityInstance.setOffsetInSentence(firstToken.getStart());
						entityInstance.setOffset(doc.translateBlocksContentOffsetToBodyOffset(blockOffset + sentenceOffset + firstToken.getStart()));
						entityInstance.setLength(doc.translateBlocksContentOffsetToBodyOffset(blockOffset + sentenceOffset + lastToken.getEnd()) - entityInstance.getOffset());
						
						List<EntityInstance> instances = new ArrayList<EntityInstance>();
						instances.add(entityInstance);
						
						np.setEntityInstances(instances);
						
						sentence.getEntityInstances().add(entityInstance);
						
						doc.getEntities().add(np);

						namedPersons.add(np);
					}
					
					
				}
				blockOffset += (blockText.length() + 1);
			
			}
			
			DocumentExtractor.updateDoc(model, doc);
			
		}
		
		return model;
	}

}
