package ai.vital.aspen.groovy

import junit.framework.TestCase

class AspenGroovyConfigTest extends TestCase {

	public void testConfig() {
		
		AspenGroovyConfig cfg = AspenGroovyConfig.get();
		cfg.reset()
		
		assertTrue(cfg.loadResourcesFromClasspath)
		
		FileInputStream fis = new FileInputStream("./conf/sample-aspen-groovy.config")
		cfg.configure(fis)
		fis.close()
		
		assertFalse(cfg.loadResourcesFromClasspath)
		
	}
	
}

