package ai.vital.aspen.groovy.test


import ai.vital.domain.*
import ai.vital.vitalservice.model.App
import ai.vital.vitalsigns.*
import ai.vital.vitalsigns.utils.BlockCompactStringSerializer

import ai.vital.common.uri.URIGenerator



class VitalBlockWriteTest {

	
	static App app
	
	static {
		app = new App(ID: 'vital-test', customerID: 'vital-ai')
	}
	
	static main(args) {
	
		Person p = new Person()
		
		p.URI = URIGenerator.generateURI(app, Person)
		
		p.name = "Marc"
		
		VitalSigns vs = VitalSigns.get()
		
		OutputStream os = new FileOutputStream(new File("testblock.vital"))
		
		
		BlockCompactStringSerializer serializer = new BlockCompactStringSerializer(new BufferedWriter(new OutputStreamWriter(os, "UTF-8")));
		
		serializer.startBlock()
		serializer.writeGraphObject(p)
		serializer.endBlock()
		
		serializer.close()
		
		
		
		
	}

}
