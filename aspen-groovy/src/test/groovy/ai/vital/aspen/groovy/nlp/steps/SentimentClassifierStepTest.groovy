package ai.vital.aspen.groovy.nlp.steps

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.nlp.models.POSTaggerModel;
import ai.vital.aspen.groovy.nlp.models.SentenceDetectorModel;
import ai.vital.aspen.groovy.nlp.models.SentimentClassifier;
import junit.framework.TestCase

class SentimentClassifierStepTest extends TestCase {

	public void testClasspath() {
		
		AspenGroovyConfig.get().loadResourcesFromClasspath = true

		SentimentClassifier.purge()		

		SentimentClassifierStep scs = new SentimentClassifierStep() 
		
		scs.init()
		
		
	}
	
	public void testDev() {
		
		AspenGroovyConfig.get().loadResourcesFromClasspath = false
		
		SentimentClassifier.purge()

		SentimentClassifierStep scs = new SentimentClassifierStep() 
		
		scs.init()
		
	}
	
}
