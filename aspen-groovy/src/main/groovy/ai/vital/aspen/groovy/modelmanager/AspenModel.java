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
import java.util.Collection;
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
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

import ai.vital.aspen.groovy.featureextraction.BinaryFeatureData;
import ai.vital.aspen.groovy.featureextraction.CategoricalFeatureData;
import ai.vital.aspen.groovy.featureextraction.DateFeatureData;
import ai.vital.aspen.groovy.featureextraction.FeatureData;
import ai.vital.aspen.groovy.featureextraction.FeatureExtraction;
import ai.vital.aspen.groovy.featureextraction.GeoLocationFeatureData;
import ai.vital.aspen.groovy.featureextraction.ImageFeatureData;
import ai.vital.aspen.groovy.featureextraction.NumericalFeatureData;
import ai.vital.aspen.groovy.featureextraction.OrdinalFeatureData;
import ai.vital.aspen.groovy.featureextraction.PredictionModelAnalyzer;
import ai.vital.aspen.groovy.featureextraction.StringFeatureData;
import ai.vital.aspen.groovy.featureextraction.TextFeatureData;
import ai.vital.aspen.groovy.featureextraction.URIFeatureData;
import ai.vital.aspen.groovy.featureextraction.WordFeatureData;
import ai.vital.aspen.groovy.taxonomy.HierarchicalCategories;
import ai.vital.aspen.groovy.taxonomy.HierarchicalCategories.TaxonomyNode;
import ai.vital.predictmodel.Aggregate;
import ai.vital.predictmodel.BinaryFeature;
import ai.vital.predictmodel.CategoricalFeature;
import ai.vital.predictmodel.DateFeature;
import ai.vital.predictmodel.DateTimeFeature;
import ai.vital.predictmodel.Feature;
import ai.vital.predictmodel.Function;
import ai.vital.predictmodel.GeoLocationFeature;
import ai.vital.predictmodel.ImageFeature;
import ai.vital.predictmodel.NumericalFeature;
import ai.vital.predictmodel.OrdinalFeature;
import ai.vital.predictmodel.Prediction;
import ai.vital.predictmodel.PredictionModel;
import ai.vital.predictmodel.StringFeature;
import ai.vital.predictmodel.Target;
import ai.vital.predictmodel.Taxonomy;
import ai.vital.predictmodel.TextFeature;
import ai.vital.predictmodel.TrainFeature;
import ai.vital.predictmodel.URIFeature;
import ai.vital.predictmodel.WordFeature;
import ai.vital.predictmodel.builder.ModelString;
import ai.vital.predictmodel.builder.ToModelImpl;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_Category;
import ai.vital.vitalsigns.model.VITAL_Container;

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
	
	protected transient FeatureExtraction _featureExtraction;
	
	protected CategoricalFeatureData trainedCategories;
	
	//this map contains taxonomies source file content
	protected Map<String, String> taxonomy2FileContent = new HashMap<String, String>();
	
	//non-supervised
	public abstract boolean isTestedWithTrainData();
	
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
		
		FeatureExtraction featureExtraction = getFeatureExtraction();
		
		Map<String, Object> features = featureExtraction.extractFeatures(input);
		
		Prediction prediction = _predict(input, features);
		
		Object output = this.modelConfig.getTarget().getFunction().call(input, features, prediction);
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
		
		
		ModelTaxonomySetter.loadTaxonomies(this, null);
		
		getFeatureExtraction();
		
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
		
		
		for(Taxonomy t : this.modelConfig.getTaxonomies()) {
			
			if(t.getTaxonomyPath() != null && t.getRootCategory() == null) throw new IOException("Taxonomy " + t.getProvides() + " was initially loaded from external file but the file wasn't found in the container");
			
		}
		
		
	}
	
	void listRecursively(List<FileStatus> target, FileStatus parentDir) throws IOException {
		
		target.add(parentDir);
		
		for(FileStatus ch : fileSystem.listStatus(parentDir.getPath()) ) {
			
			if(ch.isDirectory()) {
				
				listRecursively(target, ch);
				
			} else {
				
				target.add(ch);
				
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
			
			List<FileStatus> listStatus = new ArrayList<FileStatus>();
			
			listRecursively(listStatus, fileStatus);
//			FileStatus[] listStatus = fileSystem.listStatus(fileStatus.getPath());
			
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
				
			if(feature == null) throw new IOException("Unknown feature: " + fname + " - " + uri);
			
			FeatureData fd = null;
			
			if(feature instanceof BinaryFeature) {
				fd = new BinaryFeatureData();
			} else if(feature instanceof CategoricalFeature) {
				fd = new CategoricalFeatureData();
			} else if(feature instanceof DateFeature) {
				fd = new DateFeatureData();
			} else if(feature instanceof DateTimeFeature) {
				fd = new DateFeatureData();
			} else if(feature instanceof GeoLocationFeature) {
				fd = new GeoLocationFeatureData();
			} else if(feature instanceof ImageFeature) {
				fd = new ImageFeatureData();
			} else if(feature instanceof NumericalFeature) {
				fd = new NumericalFeatureData();
			} else if(feature instanceof OrdinalFeature) {
				fd = new OrdinalFeatureData();
			} else if(feature instanceof StringFeature) {
				fd = new StringFeatureData();
			} else if(feature instanceof TextFeature) {
				fd = new TextFeatureData();
			} else if(feature instanceof URIFeature) {
				fd = new URIFeatureData();
			} else if(feature instanceof WordFeature) {
				fd = new WordFeatureData();
			} else throw new IOException("Unhandled feature type: " + feature.getClass().getCanonicalName());
			
			String aggREsults = IOUtils.toString(inS, StandardCharsets.UTF_8.name());
			
			Map<String, Object> unwrapped = ConfigFactory.parseString(aggREsults).root().unwrapped();
			
			fd.fromJson(unwrapped);
			
			if(featuresData == null) featuresData = new HashMap<String, FeatureData>();
			
			featuresData.put(fname, fd);
			
		} else if(uri.startsWith(ModelManager.MODEL_TAXONOMIES_DIR + "/")) {
			
			String fname = uri.substring(ModelManager.MODEL_TAXONOMIES_DIR.length() + 1);
			
			if(!fname.endsWith(".txt")) throw new IOException("Taxonomy file must end with .txt");
			
			fname = URLDecoder.decode(fname.substring(0, fname.length() - 4), StandardCharsets.UTF_8.name());
			
			Taxonomy taxonomy = null;
			
			for( Taxonomy t :  this.modelConfig.getTaxonomies() ) {
				if( t.getProvides().equals(fname) ) {
					taxonomy = t; 
					break;
				}
			}
			
			if(taxonomy == null) throw new IOException("Unknown taxonomy file found: " + fname + " - " + uri );
			
			if( taxonomy.getTaxonomyPath() == null ) throw new IOException("Taxonomy " + fname + " is not loaded from external file but the file was copied into model container: " + uri);
			
			HierarchicalCategories hc = new HierarchicalCategories(inS, false);
			
			TaxonomyNode rootNode = hc.getRootNode();
			
			VITAL_Container container = new VITAL_Container();
			VITAL_Category root = ModelTaxonomySetter.processTaxonomyNode(container, null, rootNode);
			taxonomy.setRootCategory(root);
			taxonomy.setContainer(container);
			
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
		this.modelConfig.setTrainFeature(modelEl.getTrainFeature());
		
		PredictionModelAnalyzer.fixFunctionsAggregatesOrder(this.modelConfig);
		
	}

	protected boolean innerResource(String uri) {
		
		if(uri.equals(ModelManager.MODEL_BUILDER_FILE)
				|| uri.equals(ModelManager.MODEL_AGGREGATION_RESULTS_FILE)
				|| uri.startsWith(ModelManager.MODEL_FEATURES_DIR + "/")
				|| uri.startsWith(ModelManager.MODEL_TAXONOMIES_DIR + "/")
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
			
			if(trainedCategories != null) {
				
				String n = URLEncoder.encode("train-categories", StandardCharsets.UTF_8.name());
				
				File trainedFile = new File(tempDir, n + ".json");
				
				Map<String, Object> json = trainedCategories.toJSON();
				
				Config parsedMap = ConfigFactory.parseMap(json);
				
				FileUtils.writeStringToFile(trainedFile, parsedMap.root().render(), StandardCharsets.UTF_8.name());
				
			}
			
			
			//persist external taxonomies
			for(Entry<String, String> te : taxonomy2FileContent.entrySet()) {
			
				File taxDir = new File(tempDir, ModelManager.MODEL_TAXONOMIES_DIR);
				taxDir.mkdir();
				
				String n = URLEncoder.encode(te.getKey(), StandardCharsets.UTF_8.name());
				
				File taxFile = new File(taxDir, n + ".txt");
				
				FileUtils.writeStringToFile(taxFile, te.getValue(), StandardCharsets.UTF_8.name());
				
			}
			
			
			persistFiles(tempDir);
			
			//persist object file as first
			List<File> files = new ArrayList<File>( FileUtils.listFiles(tempDir, null, true) );
			
			Collections.sort(files, new Comparator<File>(){

				@Override
				public int compare(File f1, File f2) {
					
					int f1v = 0;
					int f2v = 0;
					
					if(f1.getName().equals(ModelManager.MODEL_OBJECT_FILE)) {
						f1v = -2;
					} else if(f1.getName().equals(ModelManager.MODEL_BUILDER_FILE)) {
						f1v = -1;
					}
					
					if(f2.getName().equals(ModelManager.MODEL_OBJECT_FILE)) {
						f2v = -2;
					} else if(f2.getName().equals(ModelManager.MODEL_BUILDER_FILE)) {
						f2v = -1;
					}
						
					//any order
					return new Integer(f1v).compareTo(f2v);
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
				
				IOUtils.closeQuietly(zos);

				deleteLocalCRCFile(fs, targetPath);
				
			} else {
				
				fs.mkdirs(targetPath);
				
				for(File f : files) {
					
					String name = tempDirPath.relativize(f.toPath()).toString().replace('\\', '/');
					
					Path p = new Path(targetPath, name);
					
					os1 = fs.create(p, true);
					
					FileInputStream fis = new FileInputStream(f);
					IOUtils.copy(fis, os1);
					fis.close();
					
					IOUtils.closeQuietly(os1);
					
					deleteLocalCRCFile(fs, p);
					
				}
				
			}
			
		} finally {
			IOUtils.closeQuietly(zos);
			IOUtils.closeQuietly(os1);
			FileUtils.deleteQuietly(tempDir);
		}
		
		//it's easier to dump and compress 
		
		
	}

	//a utility method that deletes crc files only from local FS 
	private void deleteLocalCRCFile(FileSystem fs, Path targetPath) {

		if(targetPath.getName().startsWith(".")) return;
		
		if(fs instanceof LocalFileSystem || fs instanceof RawLocalFileSystem ) {
			
			//delete local crc file
			Path crcPath = new Path(targetPath.getParent(),  "." + targetPath.getName() + ".crc" );
			
			try {
				fs.delete(crcPath, false);
			} catch(Exception e) {}
			
		}
		
	}

	protected abstract void persistFiles(File tempDir);

	private void writeObject(ObjectOutputStream out) throws IOException {

		if(modelConfig != null) {
			
			for(Function f : modelConfig.getFunctions() ) {
//				f.setFunction( f.getFunction().dehydrate() );
				f.setFunction(null);
			}
			
			if(modelConfig.getTrainFeature()!= null) {
//				modelConfig.setTrain(modelConfig.getTrain().dehydrate());
				modelConfig.setTrainFeature(null);
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
        loadDynamicOntologies();
    }

	private void loadDynamicOntologies() {

		List<String> domainJars = this.getModelConfig().getDomainJars();
		
		if(domainJars != null && domainJars.size() > 0) {
			AspenModelDomainsLoader loader = new AspenModelDomainsLoader();
			loader.loadDomainJars(domainJars);
		}
		
	}

	public CategoricalFeatureData getTrainedCategories() {
		return trainedCategories;
	}

	public void setTrainedCategories(CategoricalFeatureData trainedCategories) {
		this.trainedCategories = trainedCategories;
	}

	public FeatureExtraction getFeatureExtraction() {
		if(_featureExtraction == null) {
			_featureExtraction = new FeatureExtraction(modelConfig, aggregationResults);
			
			//rehydrate
			Target target = this.modelConfig.getTarget();
			if(target != null) {
				target.setFunction(target.getFunction().rehydrate(_featureExtraction, _featureExtraction, _featureExtraction));
			}
			
			TrainFeature trainFeature = this.modelConfig.getTrainFeature();
			if(trainFeature != null) {
				trainFeature.setFunction(trainFeature.getFunction().rehydrate(_featureExtraction, _featureExtraction, _featureExtraction));
			}
			
			for(Function f : this.modelConfig.getFunctions()) {
				f.setFunction(f.getFunction().rehydrate(_featureExtraction, _featureExtraction, _featureExtraction));
			}
			
		}
		return _featureExtraction;
	}

	/**
	 * overrides default algorithm config parameters
	 * it should return false when a param is not supported  
	 */
	public abstract boolean onAlgorithmConfigParam(String key, Serializable value);

	/**
	 * determines the types of features a model supports
	 * @return
	 */
	public abstract Collection<Class<? extends Feature>> getSupportedFeatures();
	
	/**
	 * must match the value in the builder
	 */
	public abstract Class<? extends Feature> getTrainFeatureType();

	public Map<String, String> getTaxonomy2FileContent() {
		return taxonomy2FileContent;
	}
	
}
