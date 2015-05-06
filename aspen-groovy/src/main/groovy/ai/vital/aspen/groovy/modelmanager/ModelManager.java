package ai.vital.aspen.groovy.modelmanager;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.vital.aspen.groovy.AspenGroovyConfig;

import com.google.common.io.Files;

/** 
 * Manages a list of loaded models
 * @author Derek
 *
 */
public class ModelManager {

	private final static Logger log = LoggerFactory.getLogger(ModelManager.class);
	
	public static final String MODEL_BUILDER_FILE = "model.builder";
	
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
		return new ArrayList<AspenModel>(loadedModels);
	}

	
	/**
	 * Unloads a model, returns true if model was found or false if not.
	 * Model is always removed from the list, despite of possible exceptions at closing.
	 * @param modelName
	 * @return
	 */
	public boolean unloadModelByName(String modelName) {
		
		for(Iterator<AspenModel> i = loadedModels.iterator(); i.hasNext(); ) {
			
			AspenModel next = i.next();
			if(next.getName().equals(modelName)) {
				i.remove();
				next.close();
				return true;
			}
			
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
		
		for(Iterator<AspenModel> i = loadedModels.iterator(); i.hasNext(); ) {
			
			AspenModel next = i.next();
			if(next.getURI().equals(modelURI)) {
				i.remove();
				next.close();
				return true;
			}
			
		}
		
		return false;
		
	}
	
	/**
	 * Loads model from given location, moel URL may be either a jar or a directory (jar is nothing else but
	 * a compressed content of that directory. Each model must model.builder file which defines the model name, URI, type
	 * features and functions
	 */
	public AspenModel loadModel(String modelURL) throws Exception {
		
		log.info("Loading model from URL: ", modelURL);
		
		Path path = new Path(modelURL);
		
		FileSystem fs = null;
		
		//obtain file systen
		
		ZipInputStream inputS = null;
		
		File tempDirectory = Files.createTempDir();
		
		tempDirectory.deleteOnExit();
		
		byte[] builderFileContent= null;
		
		InputStream builderInputStream = null;
		
		try {
			
			fs = FileSystem.get(path.toUri(), AspenGroovyConfig.get().getHadoopConfiguration());
			
			FileStatus fstatus = fs.getFileStatus(path);
			
			if(fstatus.isDirectory()) {
				
				log.info("Model URL is a directory: " + path.toString());
				
				Path builderFile = new Path(path, MODEL_BUILDER_FILE);
				
				FileStatus builderStatus = fs.getFileStatus(builderFile);
				
				if(!builderStatus.isFile()) throw new RuntimeException("model.builder file path does not denote a file: " + builderFile.toString());
				
				builderInputStream = fs.open(builderFile);
				
				builderFileContent = IOUtils.toByteArray(builderInputStream);
				
				builderInputStream.close();
				
			} else {
				
				log.info("Model URL is a file: " + path.toString());
				
				inputS = new ZipInputStream(fs.open(path));
	
				ZipEntry nextEntry = null;
				
				while ( (  nextEntry = inputS.getNextEntry() ) != null ) {
					
					String name = nextEntry.getName();
					
					if(name.equals(MODEL_BUILDER_FILE)) {
						
						ByteArrayOutputStream os = new ByteArrayOutputStream();
						IOUtils.copy(inputS, os);
						os.close();
						
						builderFileContent = os.toByteArray();
						
						break;
						
					}
					
					
				}
				
				inputS.close();
				
				if(builderFileContent == null) throw new RuntimeException("No model.builder file found in model jar/zip, path:" + path.toString());
				
			}
			
			AspenModel newModel = creator.createModel(builderFileContent);
			
			log.info("Model config loaded, name:{}, URI:{}, type:{}", new Object[]{newModel.getName(), newModel.getURI(), newModel.getType()});
			
			newModel.validateConfig();
			
			//now check if already loaded
			for(AspenModel m : loadedModels) {
				if(m.getName().equals(newModel.getName())) {
					throw new Exception("model with name: " + m.getName() + " already loaded");
				}
				if(m.getURI().equals(newModel.getURI())) {
					throw new Exception("model with URI: " + m.getURI() + " already loaded");
				}
			}
			
			log.info("Loading model {} resources...", newModel.getName());
			
			newModel.setSourceURL(modelURL);
			
			newModel.setFileStatus(fstatus);
			newModel.setFileSystem(fs);
			
			newModel.load();
			
			newModel.setFileStatus(null);
			newModel.setFileSystem(null);
			
			loadedModels.add(newModel);
			
			return newModel;
			
		} finally {
			IOUtils.closeQuietly(builderInputStream);
			IOUtils.closeQuietly(inputS);
			FileUtils.deleteQuietly(tempDirectory);
			IOUtils.closeQuietly(fs);
		}
	
		
	}
	
}
