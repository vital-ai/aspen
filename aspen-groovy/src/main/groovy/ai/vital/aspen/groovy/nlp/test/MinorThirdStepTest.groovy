package ai.vital.aspen.groovy.nlp.test

import ai.vital.vitalservice.model.App;
import ai.vital.vitalsigns.VitalSigns

import ai.vital.common.uri.URIGenerator


import ai.vital.aspen.groovy.nlp.steps.ChunkerStep
import ai.vital.aspen.groovy.nlp.steps.MinorThirdStep
import ai.vital.aspen.groovy.nlp.steps.PosTaggerStep
import ai.vital.aspen.groovy.nlp.steps.SentenceDetectorStep
import ai.vital.aspen.groovy.nlp.steps.TextExtractStep
import ai.vital.aspen.groovy.nlp.steps.WhiteSpaceTokenizerStep
import ai.vital.domain.Document
import ai.vital.domain.Sentence
import ai.vital.domain.TextBlock



class MinorThirdStepTest {

	static App app
	
	static {
		app = new App(ID: 'vitalai-demo', customerID: 'vitalai-demo')
	}
	
	
	static main(args) {
	
		// create a MinorThirdStep
		
		// pass VitalSigns Document containing text properties to step
		
		VitalSigns vs = VitalSigns.get()
		
		Document d = new Document()
		
		d.URI = URIGenerator.generateURI(app, Document)
		
		d.body = "The meeting is at 2 pm.  Hello.  Mr Smith is a great guy.  This is another sentence.  Everyone loves The Beatles.  Here is a large red ball.  He very quickly runs down the wet street."
		
		
		TextExtractStep extract_step = new TextExtractStep()
		
		def list = []
		
		
		extract_step.processDocument(d, list)
		
		println "Document: " + d
		
		println "List: " + list
		
		vs.addToCache(list)
		vs.addToCache([d])
		
		
		SentenceDetectorStep sentdetect_step = new SentenceDetectorStep()
		
		sentdetect_step.init()
		
		list = []
		
		sentdetect_step.processDocument(d, list)
		
		println "Document: " + d
		
		println "Results: " + list
		
		vs.addToCache(list)
		
		
		WhiteSpaceTokenizerStep wst_step = new WhiteSpaceTokenizerStep()
		
		
		wst_step.init()
		
		list = []
		
		wst_step.processDocument(d)

		println "Document: " + d

		d.getTextBlocks().each{ it.getSentences().each{  println "Sentence: " + it.tokensTextString    } }
				
		PosTaggerStep pos_step = new PosTaggerStep()
		
		pos_step.init()
		
		pos_step.processDocument(d)
		
		d.getTextBlocks().each{ it.getSentences().each{  println "Sentence: " + it.posTagsValuesString  } }
		
		
		ChunkerStep chunker_step = new ChunkerStep()
		
		chunker_step.init()
		
		list = []
		
		chunker_step.processDocument(d, list)
		
		vs.addToCache(list)
		
		d.getTextBlocks().each{ it.getSentences().each{  
			it.getNounPhrases().each { println "NounPhrase: " + it  } } }
		
		
		
		d.getTextBlocks().each{ it.getSentences().each{
			it.getNounPhrases().each { println "VerbPhrase: " + it  } } }
		
		
		
		MinorThirdStep m3rd_step = new MinorThirdStep()
		
		m3rd_step.init()
		
		
		list = []
		
		m3rd_step.processDocument(d, list)
		
		vs.addToCache(list)
		
		
		d.getTextBlocks().each{ it.getSentences().each{ it.getSentenceEntityInstances().each { println "Entity: " + it      }} }
		
		d.getEntities().each { println it }
		
		
			
	}

}
