package ai.vital.aspen.groovy.scripts

import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.nlp.steps.ChunkerStep
import ai.vital.aspen.groovy.nlp.steps.MinorThirdStep
import ai.vital.aspen.groovy.nlp.steps.NamedPersonStep;
import ai.vital.aspen.groovy.nlp.steps.PosTaggerStep
import ai.vital.aspen.groovy.nlp.steps.SentenceDetectorStep
import ai.vital.aspen.groovy.nlp.steps.TextExtractStep
import ai.vital.aspen.groovy.nlp.steps.WhiteSpaceTokenizerStep
import ai.vital.common.uri.URIGenerator;
import ai.vital.domain.Document
import ai.vital.domain.Edge_hasEntityInstance;
import ai.vital.domain.Entity
import ai.vital.domain.EntityInstance;
import ai.vital.property.IProperty;
import ai.vital.property.StringProperty;
import ai.vital.property.URIProperty;
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.BlockIterator;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.GraphObject;


import java.util.Map.Entry


import org.apache.commons.io.IOUtils;

/**
 * The script that process given documents.
 * Annotations data is persisted in a block file
 *   
 * @author Derek
 *
 */
class EntityAnnotatorScript {

	static void error(String e) {
		System.err.println(e)
		System.exit(1)
	}

	static main(args) {

		def cli = new CliBuilder(usage: 'process-documents [options]')
		cli.with {
			h longOpt: "help", "Show usage information", args: 0, required: false
			i longOpt: "input", "input vital block file (.vital[.gz])", args: 1, required: true
			p longOpt: "property", "text property name", args: 1, required: true
			t longOpt: "type", "optional class simple name filter", args: 1, required: false
			o longOpt: "output", "output vital block file (.vital[.gz])", args: 1, required: true
			ow longOpt: "overwrite", "overwrite output file if exists", args: 0, required: false
			c longOpt: "config", "optional aspen-groovy config file path", args: 1, required: false
		}

		def options = cli.parse(args)

		if(!options || options.h) return

			String input = options.i

		String prop = options.p

		String type = options.t ? options.t : null
		
		String output = options.o

		boolean overwrite = options.ow ? true : false
		
		if( ! (input.endsWith(".vital") || input.endsWith(".vital.gz")) ){
			error("input block file name must end with .vital[.gz] : ${input}")
		}

		if(!( output.endsWith(".vital") || output.endsWith(".vital.gz"))) {
			error("output block file name must end with .vital[.gz] : ${output}")
		}

		File inputFile = new File(input)
		
		File outputFile = new File(output)
		
		File optionalConfigFile = options.c ? new File(options.c) : null
		
		println "Input: ${inputFile.absolutePath}"
		println "Property: ${prop}"
		println "Type filter: ${type}"
		println "Output: ${outputFile.absolutePath}"
		println "Overwrite: ${overwrite}"
		println "Optional config file: ${optionalConfigFile != null ? optionalConfigFile.absolutePath : null}"

		if(!inputFile.exists()) error("Input file not found: ${inputFile.absolutePath}")
		if(!inputFile.isFile()) error("Input path is not a file: ${inputFile.absolutePath}")

		if(outputFile.exists()) {

			if(!outputFile.isFile()) error("output path is not a file: ${outputFile.absolutePath}")
			
			if(!overwrite) error("Output file already exists: ${outputFile.absolutePath}")

			println "Output file already exists, will be overwritten: ${outputFile.absolutePath}"
		}

		
		if(optionalConfigFile != null) {
			println "Loading optional aspen-groovy config from file: ${optionalConfigFile.absolutePath}"
			InputStream stream = null
			try {
				stream = new FileInputStream(optionalConfigFile)
				AspenGroovyConfig.get().configure(stream)
			} finally {
				IOUtils.closeQuietly(stream)
			}
		}
		
		VitalService service = ai.vital.vitalservice.factory.VitalServiceFactory.getVitalService()
		
		VitalSigns vs = VitalSigns.get()

		
		BlockIterator src = BlockCompactStringSerializer.getBlocksIterator(inputFile);
		
		int iterated = 0
		int nonDoc = 0
		int nonType = 0
		int noField = 0
		int noValue = 0
		int outdocs = 0
		
		TextExtractStep extract_step = new TextExtractStep()
		//don't process html content, we need 1:1 text <-> entities cover
		extract_step.processHtml = false
		
		SentenceDetectorStep sentdetect_step = new SentenceDetectorStep()
		sentdetect_step.singleSentencePerBlock = true
		sentdetect_step.init()
		
		
		WhiteSpaceTokenizerStep wst_step = new WhiteSpaceTokenizerStep(true)
		wst_step.init()
		
		
		/*
		PosTaggerStep pos_step = new PosTaggerStep()
		pos_step.init()
		
		ChunkerStep chunker_step = new ChunkerStep()
		chunker_step.init()
		*/
		
		
		MinorThirdStep m3rd_step = new MinorThirdStep()
		m3rd_step.init()
		
		NamedPersonStep namePersonStep = new NamedPersonStep()
		namePersonStep.init()
		
		
		BlockCompactStringSerializer writer = new BlockCompactStringSerializer(outputFile)
		
		int entityCounter = 1
		
		while(src.hasNext()) {
			
			VitalBlock block = src.next()
			
			List<GraphObject> allObjects = [block.mainObject]
			allObjects.addAll(block.dependentObjects)
			
			
			writer.startBlock()
			for(GraphObject g : allObjects) {
				writer.writeGraphObject(g)
			}
			
			
			for(GraphObject g : allObjects) {
				
				iterated++
				
				if(!( g instanceof Document)) {
					nonDoc++
					continue
				}
				
				if(type != null && !g.getClass().getSimpleName().equals(type)) {
					nonType++
					continue
				}
				
				String text = null
				try {
					text = g[prop]
				} catch(Exception e) {
					noField++
					continue
				}
				
				if(text == null || text.isEmpty()) {
					noValue++;
					continue
				}
				
				
				vs.purgeCache()
				

				Document originalDocument = g				
				
				//empty
				Document d = new Document()
				d.generateURI(service.getApp())
				d.body = text
				
				def list = []
				
				extract_step.processDocument(d, list)
				
				vs.addToCache(list)
				vs.addToCache([d])
				
				list = []
				
				sentdetect_step.processDocument(d, list)
				vs.addToCache(list)
				
				list = []
				
				wst_step.processDocument(d)
			
				/*
				pos_step.processDocument(d)
			
				list = []
				
				chunker_step.processDocument(d, list)
				
				vs.addToCache(list)
				*/
				
				
				list = []
				
				m3rd_step.processDocument(d, list)
				
				vs.addToCache(list)
				
				
				list = []
				namePersonStep.processDocument(d, list)
				vs.addToCache(list)
				
				
				list = []
				
				String dID = uri2LocalPart(originalDocument.URI)
				
				for(Entity entity : d.getEntities()) {
					
					for( EntityInstance ei : entity.getEntityInstances()) {
						
						String localPart = "T" + entityCounter++
						
						ei.URI = URIGenerator.generateURI(service.getApp(), EntityInstance.class, dID + '_' + localPart)
						
						Edge_hasEntityInstance edge = new Edge_hasEntityInstance()
						edge.URI = URIGenerator.generateURI(service.getApp(), Edge_hasEntityInstance.class, dID + '_to_' + localPart)
						edge.addSource(originalDocument).addDestination(ei)
						
						writer.writeGraphObject(ei)
						writer.writeGraphObject(edge)
					}
					
				}
				
				outdocs++
				
			}
			
			writer.endBlock()
			
			
		}
		
		
		src.close()
		
		writer.close()

		println "iterated  : ${iterated}"
		println "noField   : ${noField}"
		println "noValue   : ${noValue}"
		println "nonDoc    : ${nonDoc}"
		println "nonType   : ${nonType}"
		println "outdocs   : ${outdocs}"


//
//
//		d.getTextBlocks().each{
//			it.getSentences().each{
//				it.getNounPhrases().each { println "NounPhrase: " + it  } } }
//
//
//
//		d.getTextBlocks().each{
//			it.getSentences().each{
//				it.getNounPhrases().each { println "VerbPhrase: " + it  } } }







	}
	
	static String uri2LocalPart(String URI) {
		
		int lastSlash = URI.lastIndexOf('/')
		
		if(lastSlash > 0 && lastSlash < URI.length() - 1) {
			return URI.substring(lastSlash + 1)
		}
		
		return URI
		
	}

}
