package ai.vital.aspen.data;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ai.vital.query.querybuilder.VitalBuilder;
import ai.vital.vitalservice.ServiceOperations;
import ai.vital.vitalsigns.domains.DifferentDomainVersionLoader;
import ai.vital.vitalsigns.domains.DifferentDomainVersionLoader.DomainWrapper;
import ai.vital.vitalsigns.model.DomainOntology;
import ai.vital.vitalsigns.ontology.OntologyProcessor;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class LoaderSingleton {

	private static DifferentDomainVersionLoader loader;
	
	private static ServiceOperations serviceOps;
	
	public static boolean OLD_DOMAIN_OUTPUT = false;
	
	public static boolean OLD_DOMAIN_INPUT = false;
	
	private static LoaderSingleton singleton = null;
	
	byte[] mainDomainBytes;
	
	List<byte[]> otherDomainBytes = new ArrayList<byte[]>();
	
	public static LoaderSingleton initParent(String owlDirectory, String owlFileName, Configuration hadoopConfig) throws Exception {
		
		if(singleton != null) throw new Exception("Singleton already initialized");
		
		singleton = new LoaderSingleton();
		
		Path owlDirectoryPath = new Path(owlDirectory);
		Path owlPath = new Path(owlDirectoryPath, owlFileName);
		
		FileSystem fs = FileSystem.get(owlDirectoryPath.toUri(), hadoopConfig);
		
		if(!fs.isDirectory(owlDirectoryPath)) throw new Exception("OWL directory path is not a directory: " + owlDirectory);
		
		for( FileStatus f : fs.listStatus(owlDirectoryPath) ) {
			
			if(f.getPath().getName().equals(owlFileName)) {
				
				InputStream ins = null;
				
				try {
					
					ins = fs.open(f.getPath());
					
					singleton.mainDomainBytes = IOUtils.toByteArray(ins); 
					
				} finally {
					IOUtils.closeQuietly(ins);
				}
				
			} else if(f.getPath().getName().endsWith(".owl")) {
				
				InputStream ins = null;
				
				try {
					
					ins = fs.open(f.getPath());
					
					singleton.otherDomainBytes.add(IOUtils.toByteArray(ins)); 
					
				} finally {
					IOUtils.closeQuietly(ins);
				}				
				
			}
			
		}
		
		return singleton;
		
		
	}
	
	public static LoaderSingleton get() {
		if(singleton == null) throw new RuntimeException("No singleton active");
		return singleton;
	}
	
	private LoaderSingleton() {

	}
	
	public static ServiceOperations getServiceOperations() {
		if(serviceOps == null) throw new RuntimeException("Service ops not set!");
		return serviceOps;
	}
	
	public static DifferentDomainVersionLoader init(byte[] mainDomainBytes, List<byte[]> otherDomainBytes, String serviceOpsContent) throws Exception {
		
		if(loader != null) return loader;
		
		synchronized(LoaderSingleton.class) {
			
			if(loader != null) return loader;
			
			loader = new DifferentDomainVersionLoader();
			
			List<DomainWrapper> otherDomains = new ArrayList<DomainWrapper>();
			for(byte[] otherDomain : otherDomainBytes) {
				
	            Model model = ModelFactory.createDefaultModel();
	            model.read(new ByteArrayInputStream(otherDomain), null);
	            DomainOntology ontologyMetaData = OntologyProcessor.getOntologyMetaData(model);
				
				otherDomains.add(new DomainWrapper(model, ontologyMetaData, otherDomain));
				
			}
			
			loader.load(mainDomainBytes, otherDomains);
			
			serviceOps = new VitalBuilder().queryString(serviceOpsContent).toService();
			
		}
		
		return loader;
		
	}
	
	public static DifferentDomainVersionLoader getActiveOutputLoader() {
		if(OLD_DOMAIN_OUTPUT) return loader;
		return null;
	}
	
	public static DifferentDomainVersionLoader getActiveInputLoader() {
		if(OLD_DOMAIN_INPUT) return loader;
		return null;
	}

	public byte[] getMainDomainBytes() {
		return mainDomainBytes;
	}

	public List<byte[]> getOtherDomainBytes() {
		return otherDomainBytes;
	}

}
