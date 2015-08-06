package ai.vital.aspen.groovy.modelmanager;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.aspen.groovy.AspenGroovyConfig;

/** 
 * Manages a list of loaded models
 * @author Derek
 *
 */
public class ModelManager {

	private final static Logger log = LoggerFactory.getLogger(ModelManager.class);
	
	public static final String MODEL_BUILDER_FILE = "model.builder";
	
	//serialized model object file
	public static final String MODEL_OBJECT_FILE = "model.object";
	
	public static final String MODEL_AGGREGATION_RESULTS_FILE = "aggregation_results.json";
	
	public static final String MODEL_FEATURES_DIR = "features";
	
	public static final String MODEL_TAXONOMIES_DIR = "taxonomies";
	
	private List<AspenModel> loadedModels = new ArrayList<AspenModel>();
	
	private ModelCreator creator = null;
	
	@SuppressWarnings("unchecked")
	public ModelManager() {
	
		//make sure all model classes are registered
		Map<String, String> modelType2Class = AspenGroovyConfig.get().modelType2Class;
		
		Map<String, Class<? extends AspenModel>> type2ModelClass = new HashMap<String, Class<? extends AspenModel>>();
		for(Entry<String, String> e : modelType2Class.entrySet()) {
			try {
				type2ModelClass.put(e.getKey(), (Class<? extends AspenModel>) Class.forName(e.getValue()));
			} catch (ClassNotFoundException e1) {
				log.warn("Model implementation class not found: " +e.getValue() + ", model type: " + e.getKey());
			}
		}
		creator = new ModelCreator(type2ModelClass);
	}
	
	
	/**
	 * Returns a list of loaded models
	 * @return
	 */
	public List<AspenModel> getLoadedModels() {
		synchronized (loadedModels) {
			return new ArrayList<AspenModel>(loadedModels);
		}
	}

	
	/**
	 * Unloads a model, returns true if model was found or false if not.
	 * Model is always removed from the list, despite of possible exceptions at closing.
	 * @param modelName
	 * @return
	 */
	public boolean unloadModelByName(String modelName) {
		
		AspenModel found = null;
		
		synchronized (loadedModels) {
			
			for(Iterator<AspenModel> i = loadedModels.iterator(); i.hasNext(); ) {
				
				AspenModel next = i.next();
				if(next.getName().equals(modelName)) {
					i.remove();
					found = next; 
					break;
				}
				
			}
			
		}
		
		if(found != null) {
			found.close();
			return true;
		}
		
		return false;
		
	}

	/**
	 * Unloads a model, returns true if model was found or false if not.
	 * Model is always removed from the list, despite of possible exceptions at closing.
	 * @param modelName
	 * @return
	 */
	public boolean unloadModelByURI(String modelURI) {
		
		AspenModel found = null;
		
		synchronized (loadedModels) {
			
			for(Iterator<AspenModel> i = loadedModels.iterator(); i.hasNext(); ) {
				AspenModel next = i.next();
				if(next.getURI().equals(modelURI)) {
					i.remove();
					found = next; 
					break;
				}
				
			}
			
			
		}
		
		if(found != null) {
			found.close();
			return true;
		}
		
		return false;
		
	}
	
	
	/**
	 * alias for {@link #loadModel(String, boolean)} with reloadIfExists=false
	 * @param modelURL
	 * @return
	 * @throws Exception
	 */
	public AspenModel loadModel(String modelURL) throws Exception {
		return loadModel(modelURL, false);
	}
	
	/**
	 * Loads model from given location, moel URL may be either a jar or a directory (jar is nothing else but
	 * a compressed content of that directory. Each model must model.object file which defines the model name, URI, type
	 * features and functions
	 * @param modelURL model location URL
	 * @param reloadIfExists  if <code>true</code> existing model with given name or URI will be replaced, throws an exception otherwise 
	 */
	public AspenModel loadModel(String modelURL, boolean reloadIfExists) throws Exception {
		
		log.info("Loading model from URL: ", modelURL);
		
		Path path = new Path(modelURL);
		
		FileSystem fs = null;
		
		//obtain file systen
		
		ZipInputStream inputS = null;
		
		byte[] objectFileContent = null;
		
		byte[] builderFileContent = null;
		
		InputStream builderInputStream = null;
		
		InputStream objectInputStream = null;
		
		try {
			
			fs = FileSystem.get(path.toUri(), AspenGroovyConfig.get().getHadoopConfiguration());
			
			FileStatus fstatus = fs.getFileStatus(path);
			
			if(fstatus.isDirectory()) {
				
				log.info("Model URL is a directory: " + path.toString());
				
				Path builderFile = new Path(path, MODEL_BUILDER_FILE);
				
				FileStatus bStatus = fs.getFileStatus(builderFile);
				
				if(!bStatus.isFile()) throw new RuntimeException(MODEL_BUILDER_FILE + " file path does not denote a file: " + builderFile.toString());
				
				Path objectFile = new Path(path, MODEL_OBJECT_FILE);
				
				FileStatus builderStatus = fs.getFileStatus(objectFile);
				
				builderInputStream = fs.open(builderFile);
				
				builderFileContent = IOUtils.toByteArray(builderInputStream);
				
				builderInputStream.close();
				
				if(!builderStatus.isFile()) throw new RuntimeException(MODEL_OBJECT_FILE + " file path does not denote a file: " + objectFile.toString());
				
				objectInputStream = fs.open(objectFile);
				
				objectFileContent = IOUtils.toByteArray(objectInputStream);
				
				objectInputStream.close();
				
			} else {
				
				log.info("Model URL is a file: " + path.toString());
				
				inputS = new ZipInputStream(fs.open(path));
	
				ZipEntry nextEntry = null;
				
				while ( (  nextEntry = inputS.getNextEntry() ) != null ) {
					
					String name = nextEntry.getName();
					
					if(name.equalsIgnoreCase(MODEL_BUILDER_FILE)) {
						
						ByteArrayOutputStream os = new ByteArrayOutputStream();
						IOUtils.copy(inputS, os);
						os.close();
						
						builderFileContent = os.toByteArray();
						
					} else if(name.equals(MODEL_OBJECT_FILE)) {
						
						ByteArrayOutputStream os = new ByteArrayOutputStream();
						IOUtils.copy(inputS, os);
						os.close();
						
						objectFileContent = os.toByteArray();
						
						
					}
					
					if(builderFileContent != null && objectFileContent != null) {
						break;
					}
					
					
				}
				
				inputS.close();
				
				if(objectFileContent == null) throw new RuntimeException("No " + MODEL_OBJECT_FILE + " file found in model jar/zip, path:" + path.toString());
				
			}
			
			//temp model
			creator.loadDomainsFromBuilder(builderFileContent);
			
			AspenModel newModel = creator.createModelFromObject(objectFileContent);
			
			log.info("Model config loaded, name:{}, URI:{}, type:{}", new Object[]{newModel.getName(), newModel.getURI(), newModel.getType()});
			
			newModel.validateConfig();
			
			//now check if already loaded
			if(!reloadIfExists) {
				synchronized (loadedModels) {
					for(AspenModel m : loadedModels) {
						if(m.getName().equals(newModel.getName())) {
							throw new Exception("model with name: " + m.getName() + " already loaded");
						}
						if(m.getURI().equals(newModel.getURI())) {
							throw new Exception("model with URI: " + m.getURI() + " already loaded");
						}
					}
				}
			}
			
			log.info("Loading model {} resources...", newModel.getName());
			
			newModel.setSourceURL(modelURL);
			
			long timestamp = fstatus.getModificationTime();
			
			newModel.setFileStatus(fstatus);
			newModel.setFileSystem(fs);
			newModel.setTimestamp(new Date(timestamp));
			
			newModel.load();
			
			newModel.setFileStatus(null);
			newModel.setFileSystem(null);
			
			
			synchronized (loadedModels) {
				
				//remove all existing models with that uri/name
				for( Iterator<AspenModel> iterator = loadedModels.iterator(); iterator.hasNext(); ) {
					AspenModel m = iterator.next();
					if(m.getName().equals(newModel.getName()) || m.getURI().equals(newModel.getURI())) {
						iterator.remove();
					}
				}
				
				loadedModels.add(newModel);
				
			}
			
			
			return newModel;
			
		} finally {
			IOUtils.closeQuietly(builderInputStream);
			IOUtils.closeQuietly(objectInputStream);
			IOUtils.closeQuietly(inputS);
//			IOUtils.closeQuietly(fs);
		}
	
		
	}
	
}
