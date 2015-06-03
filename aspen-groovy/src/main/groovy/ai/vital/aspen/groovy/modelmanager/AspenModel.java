package ai.vital.aspen.groovy.modelmanager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ai.vital.aspen.groovy.featureextraction.CategoricalFeatureData;
import ai.vital.aspen.groovy.featureextraction.FeatureData;
import ai.vital.aspen.groovy.featureextraction.FeatureExtraction;
import ai.vital.aspen.groovy.featureextraction.NumericalFeatureData;
import ai.vital.aspen.groovy.featureextraction.PredictionModelAnalyzer;
import ai.vital.aspen.groovy.featureextraction.TextFeatureData;
import ai.vital.aspen.groovy.featureextraction.WordFeatureData;
import ai.vital.predictmodel.Aggregate;
import ai.vital.predictmodel.CategoricalFeature;
import ai.vital.predictmodel.Feature;
import ai.vital.predictmodel.Function;
import ai.vital.predictmodel.NumericalFeature;
import ai.vital.predictmodel.Prediction;
import ai.vital.predictmodel.PredictionModel;
import ai.vital.predictmodel.TextFeature;
import ai.vital.predictmodel.WordFeature;
import ai.vital.predictmodel.builder.ModelString;
import ai.vital.predictmodel.builder.ToModelImpl;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock;
import ai.vital.vitalsigns.model.GraphObject;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public abstract class AspenModel implements Serializable {

//	protected static ObjectMapp
	
	private static final long serialVersionUID = 1L;

	protected String name;
	
	protected String URI;

	protected String sourceURL;
	
	private boolean loaded = false;
	
	protected PredictionModel modelConfig;

	transient protected FileStatus fileStatus;
	
	transient protected FileSystem fileSystem;

	private Date timestamp;
	
	private String builderContent;
	
	//data collected during training
	protected Map<String, FeatureData> featuresData;
	
	protected Map<String, Double> aggregationResults;
	
	protected transient FeatureExtraction featureExtraction;
	
	protected CategoricalFeatureData trainedCategories;
	
	//returns true if a model returns predictions as categories
	public abstract boolean isCategorical();
	
	//returns true if input data should be split into train + test
	public abstract boolean isSupervised();
	
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

	
	public PredictionModel getModelConfig() {
		return modelConfig;
	}
	
	public String getBuilderContent() {
		return builderContent;
	}

	public void setBuilderContent(String builderContent) {
		this.builderContent = builderContent;
	}

//	/**
//	 * Returns prediction results with confidence, sorted in descending order
//	 * @param input
//	 * @return
//	 */
//	public abstract List<GraphObject> predict(List<GraphObject> input);
	
	public final List<GraphObject> predict(List<GraphObject> input) {
		return this.predict(new VitalBlock(input));
	}
	
	/**
	 * The output list contains either updated objects or new only (diff to the input block)
	 * @param input
	 * @return
	 */
	public final List<GraphObject> predict(VitalBlock input) {
		
		if(featureExtraction == null) {
			this.featureExtraction = new FeatureExtraction(modelConfig, aggregationResults);
		}
		
		Map<String, Object> features = featureExtraction.extractFeatures(input);
		
		Prediction prediction = _predict(input, features);
		
		Object output = this.modelConfig.getTarget().call(input, features, prediction);
		if(!( output instanceof List)) throw new RuntimeException("Model target output should be a list of graph objects");
	
		return (List<GraphObject>) output;
	}
	
	
	/**
	 * 
	 * @param input
	 * @param features
	 * @return
	 */
	protected abstract Prediction _predict(VitalBlock input, Map<String, Object> features);
	
	public final void load() throws Exception {
		
		
		if(loaded) throw new Exception("Model " + name + " already loaded");
		
		_load();
		
		
		innerValidation();
		
		onResourcesProcessed();
		
		
		this.featureExtraction = new FeatureExtraction(modelConfig, aggregationResults);
		
		loaded = true;
		
		
		
		
	}

	private void innerValidation() throws IOException {

		if(builderContent == null) throw new IOException("No builder content deserialized");
		
		if(featuresData == null) throw new IOException("No features deserialized");
		
		for(Feature f : modelConfig.getFeatures()) {
			if(!featuresData.containsKey(f.getName())) throw new IOException("No feature data found for name: " + f.getName());
		}
		
		
		List<Aggregate> aggregates = modelConfig.getAggregates();
		if(aggregates != null) {
			
			for(Aggregate a : aggregates) {
				if(aggregationResults == null || !aggregationResults.containsKey(a.getProvides())) throw new IOException("No aggregates data found for name: " + a.getProvides());
			}
			
		}
		
		
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
				
				boolean innerResource = innerResource(uri);
				
				if(innerResource || acceptResource(uri)) {
					
					FSDataInputStream inS = null;
					
					try {
						
						inS = fileSystem.open(fs.getPath());
						
						if(innerResource) {
							
							processInnerResource(uri, inS);
							
						} else {
							
							processResource(uri, inS);
							
						}
						
						
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
					
					if(innerResource(uri)) {
						
						processInnerResource(uri, inputS);
						
					} else if(acceptResource(uri)) {
						
						processResource(uri, inputS);
						
					}
					
				}
				
			} finally {
				IOUtils.closeQuietly(inputS1);
				IOUtils.closeQuietly(inputS);
			}
			
		}
		
	};
	
	
	private void processInnerResource(String uri, InputStream inS) throws IOException {

		if(uri.equals(ModelManager.MODEL_BUILDER_FILE)) {
			
			String builderString = IOUtils.toString(inS, StandardCharsets.UTF_8.name());
			
			if(this.builderContent != null) {
				
				if(!builderContent.equals(builderString)) throw new IOException("model builder file string and saved bean string are different!");
				
			}
			
			this.builderContent = builderString;
			
			restoreClosures();
			
		} else if(uri.equals(ModelManager.MODEL_AGGREGATION_RESULTS_FILE)) {
			
			String aggREsults = IOUtils.toString(inS, StandardCharsets.UTF_8.name());
			
			Map unwrapped = ConfigFactory.parseString(aggREsults).root().unwrapped();
			
			this.aggregationResults = unwrapped;
			
		} else if(uri.startsWith(ModelManager.MODEL_FEATURES_DIR + "/")) {
			
			String fname = uri.substring(ModelManager.MODEL_FEATURES_DIR.length() + 1);
			
			if(!fname.endsWith(".json")) throw new IOException("Feature json file must end with .json");
			
			fname = URLDecoder.decode(fname.substring(0, fname.length() -5), StandardCharsets.UTF_8.name());
			
			Feature feature = null;
			for( Feature  f :  this.modelConfig.getFeatures() ) {
				if( f.getName().equals(fname) ) {
					feature = f;
					break;
				}
			}
				
			if(feature == null) throw new IOException("Unknown feature: " + fname);
			
			FeatureData fd = null;
			
			if(feature instanceof TextFeature) {
				fd = new TextFeatureData();
			} else if(feature instanceof WordFeature) {
				fd = new WordFeatureData();
			} else if(feature instanceof CategoricalFeature) {
				fd = new CategoricalFeatureData();
			} else if(feature instanceof NumericalFeature) {
				fd = new NumericalFeatureData();
			} else throw new IOException("Unhandled feature type: " + feature.getClass().getCanonicalName());
			
			String aggREsults = IOUtils.toString(inS, StandardCharsets.UTF_8.name());
			
			Map<String, Object> unwrapped = ConfigFactory.parseString(aggREsults).root().unwrapped();
			
			fd.fromJson(unwrapped);
			
			if(featuresData == null) featuresData = new HashMap<String, FeatureData>();
			
			featuresData.put(fname, fd);
			
		} else throw new IOException("Unhandled inner model resource: " + uri);
		
	}

	/**
	 * restores closures from builder file, closures are not serializable
	 */
	protected void restoreClosures() {

		ModelString modelString = new ModelString();
		modelString.setModelString(builderContent);
		
		PredictionModel modelEl = new ToModelImpl().toModel(modelString.toModel());
		//we need to copy closures from builder file
		
		this.modelConfig.setFunctions(modelEl.getFunctions());
		this.modelConfig.setTarget(modelEl.getTarget());
		this.modelConfig.setTrain(modelEl.getTrain());
		
		PredictionModelAnalyzer.fixFunctionsAggregatesOrder(this.modelConfig);
		
	}

	protected boolean innerResource(String uri) {
		
		if(uri.equals(ModelManager.MODEL_BUILDER_FILE)
				|| uri.equals(ModelManager.MODEL_AGGREGATION_RESULTS_FILE)
				|| uri.startsWith(ModelManager.MODEL_FEATURES_DIR + "/")
				) {
			return true;
		}

		return false;
		
	}
	
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
	

	public void setModelElement(PredictionModel modelEl) {
		this.modelConfig = modelEl;
	}
	
	/**
	 * 
	 * @throws Exception
	 */
	public abstract void validateConfig() throws Exception;

	/**
	 * Closes the model and releases all resources.
	 * Should be overridden by subclasses.
	 */
	public void close() {
		
	}

	/**
	 * Set model timestamp (from resource)
	 * @param timestamp
	 */
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * Returns model timestamp (copied from resource modification timestamp)
	 * @return
	 */
	public Date getTimestamp() {
		return timestamp;
	}

	public Map<String, FeatureData> getFeaturesData() {
		return featuresData;
	}

	public void setFeaturesData(Map<String, FeatureData> featuresData) {
		this.featuresData = featuresData;
	}

	public Map<String, Double> getAggregationResults() {
		return aggregationResults;
	}

	public void setAggregationResults(Map<String, Double> aggregationResults) {
		this.aggregationResults = aggregationResults;
	}
	
	/**
	 * Persists the model at given location. The output path must not exist.
	 * @param fs
	 * @param targetPath
	 * @param asJar
	 * @throws IOException 
	 */
	public void persist(FileSystem fs, Path targetPath, boolean asJar) throws IOException {

		if ( fs.exists(targetPath) ) throw new IOException("Output path already exists: " + targetPath.toString());

		if(builderContent == null || builderContent.isEmpty()) throw new IOException("Builder content string not set!");
		
		
		//validation phase
		
		List<Aggregate> aggregates = modelConfig.getAggregates();
		if(aggregates != null) {
			for(Aggregate a : aggregates) {
				if(aggregationResults == null || !aggregationResults.containsKey(a.getProvides())) throw new IOException("No aggregation result: " + a.getProvides() + " function: " + a.getFunction());
			}
		}
		
		if(featuresData == null) throw new IOException("No features data");
		
		
		for(Feature f : modelConfig.getFeatures()) {
			
			if( ! featuresData.containsKey(f.getName()) ) throw new IOException("No data for feature: " + f.getName());
			
		}
		
		File tempDir = null;
		
		OutputStream os1 = null;
		ZipOutputStream zos = null;
				
		
		try {
			
			tempDir = Files.createTempDirectory("aspenmodel").toFile();
			
			FileUtils.writeByteArrayToFile(new File(tempDir, ModelManager.MODEL_OBJECT_FILE), SerializationUtils.serialize(this));

			if(builderContent != null) {
				FileUtils.writeStringToFile(new File(tempDir, ModelManager.MODEL_BUILDER_FILE), builderContent, StandardCharsets.UTF_8.name());
			}
			
			//serialize aggregates
			if( aggregationResults != null ) {

				Config parsedMap = ConfigFactory.parseMap(aggregationResults);
				
				FileUtils.writeStringToFile(new File(tempDir, ModelManager.MODEL_AGGREGATION_RESULTS_FILE), parsedMap.root().render(), StandardCharsets.UTF_8.name());
				
			}
			
			File featuresDir = new File(tempDir, ModelManager.MODEL_FEATURES_DIR);
			featuresDir.mkdirs();
			
			for(Entry<String, FeatureData> e : featuresData.entrySet()) {
				
				String n = URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8.name());
				
				File featureFile = new File(featuresDir, n + ".json"); 
				
				Map<String, Object> json = e.getValue().toJSON();
				
				Config parsedMap = ConfigFactory.parseMap(json);
				
				FileUtils.writeStringToFile(featureFile, parsedMap.root().render(), StandardCharsets.UTF_8.name());
				
			}
			
			
			persistFiles(tempDir);
			
			//persist object file as first
			List<File> files = new ArrayList<File>( FileUtils.listFiles(tempDir, null, true) );
			
			Collections.sort(files, new Comparator<File>(){

				@Override
				public int compare(File f1, File f2) {
					if(f1.getName().equals(ModelManager.MODEL_OBJECT_FILE)) {
						return -1;
					} else if(f2.getName().equals(ModelManager.MODEL_OBJECT_FILE)) {
						return 1;
					}
						
					//any order
					return 0;
				}});
			
			java.nio.file.Path tempDirPath = tempDir.toPath();
			
			if(asJar) {
				
				os1 = fs.create(targetPath, true);
				zos = new ZipOutputStream(os1, StandardCharsets.UTF_8);
				
				for(File f : files) {
					
					String name = tempDirPath.relativize(f.toPath()).toString().replace('\\', '/');
					
					zos.putNextEntry(new ZipEntry(name));
					
					FileInputStream fis = new FileInputStream(f);
					IOUtils.copy(fis, zos);
					fis.close();
					
				}
				
			} else {
				
				fs.mkdirs(targetPath);
				
				for(File f : files) {
					
					String name = tempDirPath.relativize(f.toPath()).toString().replace('\\', '/');
					
					os1 = fs.create(new Path(targetPath, name), true);
					
					FileInputStream fis = new FileInputStream(f);
					IOUtils.copy(fis, os1);
					fis.close();
					
					IOUtils.closeQuietly(os1);
					
				}
				
			}
			
		} finally {
			IOUtils.closeQuietly(zos);
			IOUtils.closeQuietly(os1);
			FileUtils.deleteQuietly(tempDir);
		}
		
		//it's easier to dump and compress 
		
		
	}

	protected abstract void persistFiles(File tempDir);

	private void writeObject(ObjectOutputStream out) throws IOException {

		if(modelConfig != null) {
			
			for(Function f : modelConfig.getFunctions() ) {
//				f.setFunction( f.getFunction().dehydrate() );
				f.setFunction(null);
			}
			
			if(modelConfig.getTrain() != null) {
//				modelConfig.setTrain(modelConfig.getTrain().dehydrate());
				modelConfig.setTrain(null);
			}
			
			if(modelConfig.getTarget() != null) {
//				modelConfig.setTarget( modelConfig.getTarget().dehydrate() );
				modelConfig.setTarget(null);
			}
			
		}
		
		out.defaultWriteObject();
		
	}
	
    private void readObject(ObjectInputStream in) throws IOException,ClassNotFoundException {
        in.defaultReadObject();
        restoreClosures();
    }

	public CategoricalFeatureData getTrainedCategories() {
		return trainedCategories;
	}

	public void setTrainedCategories(CategoricalFeatureData trainedCategories) {
		this.trainedCategories = trainedCategories;
	}

	
}
