package ai.vital.aspen.groovy.modelmanager;

import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import ai.vital.aspen.groovy.modelmanager.domain.Model;
import ai.vital.vitalsigns.model.GraphObject;

public abstract class AspenModel implements Serializable {

	private static final long serialVersionUID = 1L;

	protected String name;
	
	protected String URI;

	protected String sourceURL;
	
	private boolean loaded = false;
	
	protected Model modelConfig;

	transient protected FileStatus fileStatus;
	
	transient protected FileSystem fileSystem;
	
	public String getName() {
		return modelConfig.getName();
	}

	public String getURI() {
		return modelConfig.getURI();
	}

	public String getSourceURL() {
		return sourceURL;
	}

	public void setSourceURL(String sourceURL) {
		this.sourceURL = sourceURL;
	}
	

	public String getType() {
		return modelConfig.getType();
	}
	
	public void setFileStatus(FileStatus fileStatus) {
		this.fileStatus = fileStatus;
	}

	public void setFileSystem(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	
	public Model getModelConfig() {
		return modelConfig;
	}

	/**
	 * Returns prediction results with confidence, sorted in descending order
	 * @param input
	 * @return
	 */
	public abstract List<GraphObject> predict(List<GraphObject> input);
	
	
	public final void load() throws Exception {
		
		
		if(loaded) throw new Exception("Model " + name + " already loaded");
		
		_load();
		
		onResourcesProcessed();
		
		loaded = true;
		
		
		
		
	}

	/**
	 * Default main model loading method, it iterates over resources and calls model resource handler.
	 * If for some reason the model resources loading order matters override it and use #fileStatus field 
	 *  
	 */
	protected void _load() throws Exception {

		if( fileStatus.isDirectory() ) {
			
			FileStatus[] listStatus = fileSystem.listStatus(fileStatus.getPath());
			
			String baseURI = fileStatus.getPath().toUri().toString();
			
			for(FileStatus fs : listStatus) {

				String uri = fs.getPath().toUri().toString();
				
				uri = uri.substring(baseURI.length());

				if(uri.startsWith("/")) uri = uri.substring(1);
				
				if(acceptResource(uri)) {
					
					FSDataInputStream inS = null;
					
					try {
						
						inS = fileSystem.open(fs.getPath());
						
						processResource(uri, inS);
						
					} finally {
						IOUtils.closeQuietly(inS);
					}
					
				}
				
			}
			
		} else {
			
			//assume zip
			FSDataInputStream inputS1 = null;
			ZipInputStream inputS = null;
			try {
				
				inputS1 = fileSystem.open(fileStatus.getPath());
				
				inputS = new ZipInputStream(inputS1);
				
				
				ZipEntry nextEntry = null;
				
				while ( (  nextEntry = inputS.getNextEntry() ) != null ) {
					
					String uri = nextEntry.getName();
					
					if(acceptResource(uri)) {
						
						processResource(uri, inputS);
						
					}
					
				}
				
			} finally {
				IOUtils.closeQuietly(inputS1);
				IOUtils.closeQuietly(inputS);
			}
			
		}
		
		onResourcesProcessed();
		
		
	};
	
	/**
	 * @param name (uri relative to directory/jar) 
	 * @return
	 */
	protected abstract boolean acceptResource(String name);

	/**
	 * @param name (uri relative to directory/jar) 
	 * @return
	 * @throws Exception 
	 */
	protected abstract void processResource(String name, InputStream inputStream) throws Exception;
	
	
	protected abstract void onResourcesProcessed() throws Exception;
	

	public void setModelElement(Model modelEl) {
		this.modelConfig = modelEl;
	}
	
	public abstract void validateConfig() throws Exception;

	/**
	 * Closes the model and releases all resources.
	 * Should be overridden by subclasses.
	 */
	public void close() {
		
	}

}
