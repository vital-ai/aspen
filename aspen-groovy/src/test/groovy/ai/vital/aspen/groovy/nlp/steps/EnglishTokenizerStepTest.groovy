package ai.vital.aspen.groovy.nlp.steps

import org.junit.After;

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.nlp.models.EnglishTokenizerModel;
import junit.framework.TestCase

class EnglishTokenizerStepTest extends TestCase {

	public void testClasspath() {
		
		AspenGroovyConfig.get().loadResourcesFromClasspath = true
		
		EnglishTokenizerModel.purge()
		
		EnglishTokenizerStep ets = new EnglishTokenizerStep()
		ets.init()
		
	}
	
	public void testDev() {
		
		AspenGroovyConfig.get().loadResourcesFromClasspath = false
		
		EnglishTokenizerModel.purge()
		
		EnglishTokenizerStep ets = new EnglishTokenizerStep()
		ets.init()
		
	}
	
}
