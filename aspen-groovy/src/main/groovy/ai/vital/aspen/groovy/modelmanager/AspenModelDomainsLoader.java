package ai.vital.aspen.groovy.modelmanager;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.predictmodel.builder.ModelDomainsLoader;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.model.DomainModel;

public class AspenModelDomainsLoader implements ModelDomainsLoader {

	private final static Logger log = LoggerFactory.getLogger(AspenModelDomainsLoader.class);
	
	static void println(Object o) { System.out.println(o); }
	
	@Override
	public void loadDomainJars(List<String> urls) {

		log.info("Domain jars URLs: [{}]: {}", urls.size(), urls);
		
		List<DomainModel> dms = VitalSigns.get().getDomainModels();
		
		for(String u : urls) {
			
			Path p = new Path(u);
			
			log.info("Loading domain jar {}", p);
			println("Loading domain jar " +  p);
			
			boolean alreadyLoaded = false;
			for(DomainModel dm : dms) {
				
				String n = dm.getProperty("name") != null ? dm.getProperty("name").toString() : null;
				
				if(p.getName().equals(n)) {
					alreadyLoaded = true;
					break;
				}
				
			}
			
			if(alreadyLoaded) {
				log.info("Domain jar " + p + " seems to be already loaded");
				println("Domain jar " + p + " seems to be already loaded");
				continue;
			}
			
			InputStream inputStream = null;
			try {
				
				FileSystem fs = FileSystem.get(p.toUri(), new Configuration());
				
				inputStream = fs.open(p);
						
				File tempDir = Files.createTempDirectory("domains").toFile();
				File tempFile = new File(tempDir, p.getName()); 
				FileUtils.copyInputStreamToFile(inputStream, tempFile);
				
				tempFile.deleteOnExit();
				tempDir.deleteOnExit();
				
				VitalSigns.get().registerOntology(tempFile.toURI().toURL());
				
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				
				IOUtils.closeQuietly(inputStream);
				
			}
		}
		
		log.info("All domain jars loaded");
        
		

	}

}
