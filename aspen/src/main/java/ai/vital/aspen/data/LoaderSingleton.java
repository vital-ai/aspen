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

	private DifferentDomainVersionLoader loader;
	
	public ServiceOperations serviceOps;
	
	public static boolean OLD_DOMAIN_OUTPUT = false;
	
	public static boolean OLD_DOMAIN_INPUT = false;
	
	private static LoaderSingleton parentSingleton = null;
	
	private static LoaderSingleton childSingleton = null;
	
	byte[] mainDomainBytes;
	
	List<byte[]> otherDomainBytes = new ArrayList<byte[]>();
	
	public static void cleanup() {
	
		if(parentSingleton != null) {
			parentSingleton.doCleanup();
			parentSingleton= null;
		}
		
		if(childSingleton != null) {
			childSingleton.doCleanup();
			childSingleton = null;
		}
		
	}
	
	private void doCleanup() {
		
		if(loader != null) {
			try {
				loader.cleanup();
			} catch(Exception e) {
				e.printStackTrace();
			}
			loader = null;
			serviceOps = null;
		}

	}
	
	
	public static LoaderSingleton getParent() {
		if(parentSingleton == null) throw new RuntimeException("parent singleton not initialized");
		return parentSingleton;
		
	}
	public static LoaderSingleton initParent(String owlDirectory, String owlFileName, Configuration hadoopConfig) throws Exception {
		
		if(parentSingleton != null) {
			System.out.println("WARNING: removing left-over parent domain loader singleton");
			cleanup();
		}
		
		parentSingleton = new LoaderSingleton();
		
		Path owlDirectoryPath = new Path(owlDirectory);
		
		FileSystem fs = FileSystem.get(owlDirectoryPath.toUri(), hadoopConfig);
		
		if(!fs.isDirectory(owlDirectoryPath)) throw new Exception("OWL directory path is not a directory: " + owlDirectory);
		
		for( FileStatus f : fs.listStatus(owlDirectoryPath) ) {
			
			if(f.getPath().getName().equals(owlFileName)) {
				
				InputStream ins = null;
				
				try {
					
					ins = fs.open(f.getPath());
					
					parentSingleton.mainDomainBytes = IOUtils.toByteArray(ins); 
					
				} finally {
					IOUtils.closeQuietly(ins);
				}
				
			} else if(f.getPath().getName().endsWith(".owl")) {
				
				InputStream ins = null;
				
				try {
					
					ins = fs.open(f.getPath());
					
					parentSingleton.otherDomainBytes.add(IOUtils.toByteArray(ins)); 
					
				} finally {
					IOUtils.closeQuietly(ins);
				}				
				
			}
			
		}
		
		return parentSingleton;
		
		
	}
	
	private LoaderSingleton() {

	}
	
	public static LoaderSingleton getChild(byte[] mainDomainBytes, List<byte[]> otherDomainBytes, String serviceOpsContent) throws Exception {
		
		if(childSingleton != null) {
			return childSingleton;
		}
		
		synchronized(LoaderSingleton.class) {
			
			if(childSingleton != null) return childSingleton;

			childSingleton = new LoaderSingleton();
			
			childSingleton.loader = new DifferentDomainVersionLoader();
			
			List<DomainWrapper> otherDomains = new ArrayList<DomainWrapper>();
			for(byte[] otherDomain : otherDomainBytes) {
				
				Model model = ModelFactory.createDefaultModel();
				model.read(new ByteArrayInputStream(otherDomain), null);
				DomainOntology ontologyMetaData = OntologyProcessor.getOntologyMetaData(model);
				
				otherDomains.add(new DomainWrapper(model, ontologyMetaData, otherDomain));
				
			}
			
			childSingleton.loader.load(mainDomainBytes, otherDomains);
			
			childSingleton.serviceOps = new VitalBuilder().queryString(serviceOpsContent).toService();
			
		}
		
		return childSingleton;
		
	}
	
	public static DifferentDomainVersionLoader getActiveOutputLoader() {
		if(OLD_DOMAIN_OUTPUT && childSingleton != null) return childSingleton.loader;
		return null;
	}
	
	public static DifferentDomainVersionLoader getActiveInputLoader() {
		if(OLD_DOMAIN_INPUT && childSingleton != null) return childSingleton.loader;
		return null;
	}

	public byte[] getMainDomainBytes() {
		return mainDomainBytes;
	}

	public List<byte[]> getOtherDomainBytes() {
		return otherDomainBytes;
	}

	public DifferentDomainVersionLoader getLoader() {
		return loader;
	}

	public ServiceOperations getServiceOps() {
		return serviceOps;
	}

}
