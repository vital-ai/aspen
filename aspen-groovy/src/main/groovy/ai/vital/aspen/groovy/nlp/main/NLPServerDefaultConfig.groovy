/*******************************************************************************
 * Copyright 2014 by Vital AI, LLC . All rights reserved.
 * 
 * This software may only be used under a commercial license agreement obtained by Vital AI, LLC.
 * Vital AI, LLC may be contacted via: legal@vital.ai
 * or via contact information found at the web address: http://vital.ai/contact.html
 ******************************************************************************/
package ai.vital.aspen.groovy.nlp.main;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

import ai.vital.aspen.groovy.nlp.config.MinorthirdConfig;
import ai.vital.aspen.groovy.nlp.config.NLPServerConfig;
import ai.vital.aspen.groovy.nlp.config.OpenCalaisConfig;
import ai.vital.aspen.groovy.nlp.config.OpenNLPConfig;
import ai.vital.aspen.groovy.nlp.config.Workflows;
import ai.vital.aspen.groovy.nlp.engine.NLPServerEngine;
import ai.vital.aspen.groovy.nlp.workflowstep.AbbreviationsWorkflowStep;
import ai.vital.aspen.groovy.nlp.workflowstep.ChunkerWorkflowStep;
import ai.vital.aspen.groovy.nlp.workflowstep.LanguageDetectorWorkflowStep;
import ai.vital.aspen.groovy.nlp.workflowstep.MinorthirdWorkflowStep;
import ai.vital.aspen.groovy.nlp.workflowstep.NamedPersonWorkflowStep;
//import ai.vital.aspen.groovy.nlp.workflowstep.OpenCalaisWorkflowStep;
//import ai.vital.aspen.groovy.nlp.workflowstep.OpenCalaisWorkflowStepV2;
import ai.vital.aspen.groovy.nlp.workflowstep.PosTaggerWorkflowStep;
import ai.vital.aspen.groovy.nlp.workflowstep.SentenceDetectorWorkflowStep;
import ai.vital.aspen.groovy.nlp.workflowstep.SentimentClassifierWorkflowStep;
import ai.vital.aspen.groovy.nlp.workflowstep.TextExtractionWorkflowStep;
import ai.vital.aspen.groovy.nlp.workflowstep.WhitespaceTokenizerWorkflowStep;
import ai.vital.flow.server.config.ProcessServerConfig;
import ai.vital.flow.server.config.ProcessServerConfigManager;
import ai.vital.flow.server.main.GenerateDefaultServerConfiguration;
import ai.vital.workflow.WorkflowConfig;

public class NLPServerDefaultConfig {

	public static void main(String[] args) throws Exception {
		
		if(args.length <1) {
			System.err.println("Usage: " + NLPServerDefaultConfig.class.getCanonicalName() + " <output_xml_file>");
			return;
		}
		
		File outputFile = new File(args[0]);
		
		GenerateDefaultServerConfiguration.main([
			NLPServerEngine.class.getCanonicalName(), 
			outputFile.getAbsolutePath()
		]);

		NLPServerConfig c = new NLPServerConfig();
		ArrayList<WorkflowConfig> ws = new ArrayList<WorkflowConfig>();
		
		
		
		ws.add(new WorkflowConfig(Workflows.OpenCalaisV2, new ArrayList<WorkflowConfig.StepName>(
				Arrays.asList(
						TextExtractionWorkflowStep.TEXTEXTRACTION,
						LanguageDetectorWorkflowStep.LANGUAGEDETECTOR,
						SentenceDetectorWorkflowStep.SENTENCEDETECTOR,
						WhitespaceTokenizerWorkflowStep.WHITESPACETOKENIZER,
						PosTaggerWorkflowStep.POSTAGGER,
						ChunkerWorkflowStep.CHUNKER,
						AbbreviationsWorkflowStep.ABBREVIATIONS,
						MinorthirdWorkflowStep.MINORTHIRD,
						NamedPersonWorkflowStep.NAMEDPERSONTAGGER,
						
						SentimentClassifierWorkflowStep.SENTIMENTCLASSIFIER
						))));
		
		/*
		ws.add(new WorkflowConfig(Workflows.OpenCalais_VS, new ArrayList<WorkflowConfig.StepName>(
				Arrays.asList(
						TextExtractionWorkflowStep_VS.TEXTEXTRACTION_VS,
						LanguageDetectorWorkflowStep_VS.LANGUAGEDETECTOR_VS,
						SentenceDetectorWorkflowStep_VS.SENTENCEDETECTOR_VS,
						WhitespaceTokenizerWorkflowStep_VS.WHITESPACETOKENIZER_VS,
						PosTaggerWorkflowStep_VS.POSTAGGER_VS,
						ChunkerWorkflowStep_VS.CHUNKER_VS,
						AbbreviationsWorkflowStep_VS.ABBREVIATIONS_VS,
						MinorthirdWorkflowStep_VS.MINORTHIRD_VS,
						NamedPersonWorkflowStep_VS.NAMEDPERSONTAGGER_VS,
						OpenCalaisWorkflowStep_VS.OPENCALAIS_VS,
						SentimentClassifierWorkflowStep_VS.SENTIMENTCLASSIFIER_VS
						))));
		
		*/
		
		/*
		ws.add(new WorkflowConfig(Workflows.OpenCalais_VS, new ArrayList<WorkflowConfig.StepName>(
				Arrays.asList(
						TextExtractionWorkflowStep_VS.TEXTEXTRACTION_VS
				))));
		*/
		c.setWorkflows(ws);
		
		Class<NLPServerEngine> engineClass = NLPServerEngine.class;
		ProcessServerConfig config = ProcessServerConfigManager.generateDefaultConfig(engineClass, c);
		config.setId("NLPServer");
		config.getJms().setQueueName("nlp-step");
		
		OpenCalaisConfig ocConfig = new OpenCalaisConfig();

		ocConfig.setApiKey("! SET ME !");
		ocConfig.setConnectTimeout(5000);
		ocConfig.setReadTimeout(15000);
		
		c.setOpenCalais(ocConfig);
		
		
		OpenNLPConfig openNLP = new OpenNLPConfig();
		openNLP.setModelsDir("models");
		openNLP.setChunkerModel("en-chunker.bin");
		openNLP.setEnglishTokenizerModel("en-token.bin");
		openNLP.setPosTaggerModel("en-pos-maxent.bin");
		openNLP.setSentencesModel("en-sent.bin");
		openNLP.setSentimentModel("en-sentiment.bin");
		openNLP.setNamedPersonModel("en-ner-person.bin");
		c.setOpenNLP(openNLP);
		
		MinorthirdConfig minorthird = new MinorthirdConfig();
		minorthird.setMainMixupFile("email.mixup");
		minorthird.setMixupDir("mixup");
		c.setMinorthird(minorthird);
		
		ProcessServerConfigManager.writeToFile(config, outputFile.getAbsolutePath());
		
		
	}
	
}
