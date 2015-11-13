package ai.vital.aspen.groovy.nlp.steps


import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cybozu.labs.langdetect.Detector
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.util.LangProfile

import net.arnx.jsonic.JSON;
import net.arnx.jsonic.JSONException;
import ai.vital.vitalsigns.uri.URIGenerator;
import com.vitalai.domain.nlp.Annotation
import com.vitalai.domain.nlp.Document
import com.vitalai.domain.nlp.Edge_hasAnnotation;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.classes.ClassMetadata
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.properties.PropertiesRegistry;
import ai.vital.vitalsigns.properties.PropertyMetadata;
import ai.vital.aspen.groovy.AspenGroovyConfig;
import ai.vital.aspen.groovy.nlp.model.DocumentUtils;
import ai.vital.aspen.groovy.nlp.model.EdgeUtils;
import ai.vital.aspen.groovy.ontology.VitalOntology
import ai.vital.vitalsigns.VitalSigns

class LanguageDetectorStep {

	private final static Logger log = LoggerFactory.getLogger(LanguageDetectorStep.class);
	
	public final static String LANGUAGEDETECTOR2_VS = "languagedetector2_vs";
	
	public static boolean loaded = false;
	
	static String langs = """\
af
ar
bg
bn
cs
da
de
el
en
es
et
fa
fi
fr
gu
he
hi
hr
hu
id
it
ja
kn
ko
lt
lv
mk
ml
mr
ne
nl
no
pa
pl
pt
ro
ru
sk
sl
so
sq
sv
sw
ta
te
th
tl
tr
uk
ur
vi
zh-cn
zh-tw
"""
	
	public void init()  {
		
		if(loaded) return;
				
		log.info("Initializing Language Detector ...");
		
		
		//loading is modified, using hardcoded list
		
		String[] langsA = langs.trim().split("\\s+")
		
		log.info("Expected languages count: {}", langsA.length)
		
		int missing = 0
		
		File profileDir = null;
		
		if( ! AspenGroovyConfig.get().loadResourcesFromClasspath ) {
		
			String resDir = AspenGroovyConfig.get().resourcesDir
			if(!resDir) throw new RuntimeException("resourcesDir not set")
				
			profileDir = new File(resDir, "langdetect-profiles");
			
			log.info("Loading profies from file system: {}", profileDir.absolutePath)
			
			if(!profileDir.exists()) throw new Exception("Language profiles directory not found: ${profileDir.absolutePath}");
			if(!profileDir.isDirectory()) throw new Exception("Language profiles path is not a directory: ${profileDir.absolutePath}");
			
		} else {
		
			String path = "/resources/langdetect-profiles/"
			log.info("Loading language profile from classpath directory: ${path}, hardcoded languages set length: ${langsA.length}")
			
		}
		
		InputStream inputStream = null

		List<String> jsonProfiles = []
			
		for(String lang : langsA) {
				
			InputStream is = null;
				
			try {
					
				if ( AspenGroovyConfig.get().loadResourcesFromClasspath ) {
						 
					String path = "/resources/langdetect-profiles/${lang}"
						
						
					is = AspenGroovyConfig.class.getResourceAsStream(path)
					
					if(is == null) {
						missing++
						continue
					}
					
				} else {
				
					File f = new File(profileDir, lang)
				
					if(!f.exists()) {
						missing++
						continue
					}
					
					is = new FileInputStream(f);
					
					
				}

				String json = IOUtils.toString(is, 'UTF-8')
				
				jsonProfiles.add(json)				
				
			} catch(Exception e) {
					
			} finally {
				IOUtils.closeQuietly(is)
			}
				
		}
		
		if(jsonProfiles.size() < 1) throw new RuntimeException("No json profiles found!")
		
		DetectorFactory.loadProfile(jsonProfiles)
		
		log.info("Langdetect ${jsonProfiles.size()} profiles loaded");
		
		if(missing > 0) {
			
			log.warn("Missing ${missing} languages")
			
		}

		loaded = true;
				
	}

	public void processPayload(Document doc, List list) {
			
			String docUri = doc.getURI();
							
			log.info("Processing document {} ...", docUri);
				
			String text = DocumentUtils.getTextBlocksContent(doc);
			
			Detector detector = DetectorFactory.create();
			
			detector.append(text);
			
			String langID = "unknown";
			
			try {
				langID = detector.detect();
			} catch(Exception e) {
				log.error(e.getLocalizedMessage(), e);
			}
				
			log.info("Detected language: {}", langID);
				
			Annotation a = new Annotation();
			a.annotationName = 'language-id';
			a.annotationValue = langID;
			a.URI = docUri + "#languageAnnotation";
				
			list.addAll(Arrays.asList(a));
			list.addAll(EdgeUtils.createEdges(doc, Arrays.asList(a), Edge_hasAnnotation, VitalOntology.Edge_hasAnnotationURIBase));
				
		
	}
	
	
	public List<GraphObject> processDocument(Document doc) {
		
		String docUri = doc.getURI();
		
		log.info("Processing document {} ...", docUri);

		//get body or subproperties
		
		PropertiesRegistry pr = VitalSigns.get().getPropertiesRegistry()
		PropertyMetadata pm = pr.getPropertyByShortName(Document.class, "body")
		
		List<PropertyMetadata> bodyProperties = pr.getSubProperties(pm, true)
		
		StringBuilder sb = new StringBuilder()
		
		for( int i = bodyProperties.size() -1 ; i>=0; i-- ) {
			
			PropertyMetadata x = bodyProperties.get(i)
			
			for(ClassMetadata d : x.getDomains() ) {
				
				if(d.getClazz().isAssignableFrom(doc.getClass()) ) {
					
					def val = doc[x.shortName]
					
					if(val != null) {
						
						if(sb.length() > 0) sb.append("\n\n\n")
						sb.append(val.toString())
						
					}
					
				}
				
			}
			
		}

		
		String text = sb.toString()		
		

		Detector detector = DetectorFactory.create();

		detector.append(text);

		String langID = "unknown";

		try {
			langID = detector.detect();
		} catch(Exception e) {
			log.error(e.getLocalizedMessage(), e);
		}

		log.info("Detected language: {}", langID);

		Annotation a = new Annotation();
		a.generateURI((VitalApp) null);
		a.annotationName = 'language-id';
		a.annotationValue = langID;

		List<GraphObject> list = new ArrayList<GraphObject>()
		list.addAll(Arrays.asList(a));
		list.addAll(EdgeUtils.createEdges(doc, Arrays.asList(a), Edge_hasAnnotation, VitalOntology.Edge_hasAnnotationURIBase));
		
		return list
	}

	
	public String getName() {
		return LANGUAGEDETECTOR2_VS;
	}

	
	
	
}
