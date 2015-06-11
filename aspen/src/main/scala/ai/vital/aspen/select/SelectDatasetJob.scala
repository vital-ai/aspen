package ai.vital.aspen.select

import ai.vital.aspen.job.AbstractJob
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import com.typesafe.config.ConfigException
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.query.querybuilder.VitalBuilder
import org.apache.spark.rdd.RDD
import ai.vital.vitalservice.query.VitalSelectQuery
import ai.vital.vitalsigns.model.property.URIProperty
import scala.collection.JavaConversions._
import ai.vital.vitalsigns.VitalSigns
import java.util.Arrays
import java.util.Random
import ai.vital.hadoop.writable.VitalBytesWritable
import org.apache.hadoop.io.Text
import spark.jobserver.SparkJobValidation
import org.apache.spark.SparkContext
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobInvalid
import java.util.ArrayList
import ai.vital.vitalsigns.model.GraphObject
import spark.jobserver.SparkJobInvalid
import ai.vital.vitalsigns.model.URIReference
import ai.vital.vitalsigns.uri.URIGenerator
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock

class SelectDatasetJob {}

object SelectDatasetJob extends AbstractJob {
  
  def getJobClassName(): String = {
    classOf[SelectDatasetJob].getCanonicalName
  }

  def getJobName(): String = {
	  "Select Dataset Job"
  }
  
  def main(args: Array[String]): Unit = {
    
     _mainImpl(args)
     
  }
  
  val modelBuilderOption = new Option("b", "model-builder", true, "model builder file")
  modelBuilderOption.setRequired(true)
  
  val outputOption = new Option("o", "output", true, "output RDD[(String,Array[Byte])], either named RDD name (name:<name>) or <path> (no prefix), where path is a .vital.seq file")
  outputOption.setRequired(true)
  
  val queryOption = new Option("q", "query-file", true, "select query builder file, mutually exclusive with uris-list")
  queryOption.setRequired(false)
  
  val urisListOption = new Option("uris", "uris-list", true, "input uris list as vital.seq file containining blocks with URIReference objects or any graph objects, , mutually exclusive with query-file")
  urisListOption.setRequired(false)
  
  val limitOption = new Option("l", "limit", true, "optional documents limit (page size), default 1000")
  limitOption.setRequired(false)
  
  val percentOption = new Option("p", "percent", true, "optional output objects percent limit")
  percentOption.setRequired(false)
  
  val maxOption = new Option("max", "maxDocs", true, "optional max documents limit (hardlimit)")
  maxOption.setRequired(false)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite output file if exists")
  overwriteOption.setRequired(false)
  
  val uriOnlyOption = new Option("u", "uri-only", false, "only return uris, blocks will only contain URIRefernce objects")
  
  def getOptions(): Options = {
    addJobServerOptions(
    new Options()
    .addOption(masterOption)
    .addOption(profileOption)
    .addOption(modelBuilderOption)
    .addOption(outputOption)
    .addOption(queryOption)
    .addOption(urisListOption)
    .addOption(limitOption)
    .addOption(percentOption)
    .addOption(maxOption)
    .addOption(overwriteOption)
    .addOption(uriOnlyOption)
    )
  }

  def runJob(sc: SparkContext, jobConfig: Config): Any = {

    val queryPathParam = getOptionalString(jobConfig, queryOption)
    
    val urisListPath = getOptionalString(jobConfig, urisListOption)
    
    val outputPath = jobConfig.getString(outputOption.getLongOpt)
    
    val overwrite = jobConfig.getBoolean(overwriteOption.getLongOpt)
    
    val builderPath = new Path(jobConfig.getString(modelBuilderOption.getLongOpt))
    
    var percent = 100D
    val percentValue = getOptionalString(jobConfig, percentOption)
    if(percentValue != null) {
    	percent = java.lang.Double.parseDouble(percentValue)
    }
    
    val uriOnly = jobConfig.getBoolean(uriOnlyOption.getLongOpt)
    
    
    if(queryPathParam != null) {
    	println("query file path: " + queryPathParam)
    }
    
    if(urisListPath != null) {
    	println("uris list path: " + urisListPath);
    }
    println("output: " + outputPath)
    println("Overwrite ? " + overwrite)
    println("URI only ? " + uriOnly)
    println("builder path: " + builderPath)

    val hadoopConfiguration = new Configuration()
    
    var outputRDDName : String = null
    
    if(outputPath.startsWith("name:")) {
      outputRDDName = outputPath.substring("name:".length())
      println("output is a namedRDD: " + outputRDDName)
    }
    
    if(outputRDDName != null) {
      
    } else {
      
      val outputBlockPath = new Path(outputPath)
      
      val outpuBlockFS = FileSystem.get(outputBlockPath.toUri(), hadoopConfiguration)
      
      //check if output exists
      if(outpuBlockFS.exists(outputBlockPath)) {
        if(!overwrite) {
          throw new RuntimeException("Output file path already exists, use --overwrite option - " + outputBlockPath)
        } else {
//          if(!outpuBlockFS.isFile(outputBlockPath)) {
//            throw new RuntimeException("Output block file path exists but is not a file: " + outputBlockPath)
//          }
          outpuBlockFS.delete(outputBlockPath, true)
        }
      }
      
    }
    
    val builderFS = FileSystem.get(builderPath.toUri(), hadoopConfiguration)
    if(!builderFS.exists(builderPath)) {
      throw new RuntimeException("Builder file not found: " + builderPath.toString())
    }
    
    val builderStatus = builderFS.getFileStatus(builderPath)
    if(!builderStatus.isFile()) {
      throw new RuntimeException("Builder path does not denote a file: " + builderPath.toString())
    }
    
    
    val buildInputStream = builderFS.open(builderPath)
    val builderBytes = IOUtils.toByteArray(buildInputStream)
    buildInputStream.close()
    
    
    val modelCreator = getModelCreator()
        //not loaded!
    val aspenModel = modelCreator.createModel(builderBytes)
    
    
    
    var profile = getOptionalString(jobConfig, profileOption) 
    
    var limit = 1000
    try {
      limit = jobConfig.getInt(limitOption.getLongOpt)
    } catch {
      case ex: Exception => {}
    }
    
    if(limit <= 0) throw new RuntimeException("Limit must be greater than 0: " + limit)
    
    var maxDocs = -1
    try {
      maxDocs = jobConfig.getInt(maxOption.getLongOpt)
    }catch{ case ex: Exception=>{} }
    
    println("limit: " + limit)
    println("maxDocs: " + maxDocs)
    
    if(profile != null) {
      println("Setting custom vitalservice profile: " + profile)
      VitalServiceFactory.setServiceProfile(profile)
    } else {
      println("Using default service profile...")
    }
    
    VitalSigns.get.setVitalService( VitalServiceFactory.getVitalService )
    
    val vitalService = VitalServiceFactory.getVitalService
    
    val builder = new VitalBuilder()
    
    var offset = 0
    
    var total = 0
    
    
    var inputURIsList : RDD[(String, Array[Byte])] = null;
    
    var output : RDD[(String, Array[Byte])] = null
    
    
    val expandObjects = urisListPath != null || ( !uriOnly && ( aspenModel.getModelConfig.getFeatureQueries.size() > 0 || aspenModel.getModelConfig.getTrainQueries.size() > 0 ) )
    
  
    if(queryPathParam != null) {

      if(expandObjects) {
        
        inputURIsList = sc.parallelize(Array[(String, Array[Byte])]())
        
      } else {
        
        output = sc.parallelize(Array[(String, Array[Byte])]())
        
      }
      
      val queryPath = new Path(queryPathParam)
      println("query builder path: " + queryPath)
      val queryFS = FileSystem.get(queryPath.toUri(), new Configuration())
      
      if(!queryFS.exists(queryPath)) throw new RuntimeException("Query builder file not found: " + queryPath.toString())
      
      val queryStream = queryFS.open(queryPath)
      val queryString = IOUtils.toString(queryStream, StandardCharsets.UTF_8.name())
      queryStream.close()
      
      
      val queryObject = builder.queryString(queryString).toQuery()
      
      if(!queryObject.getClass().equals(classOf[VitalSelectQuery])) {
        throw new RuntimeException("Query string must evaluate to VitalSelectQuery (exact)")
      }
      
      val selectQuery = queryObject.asInstanceOf[VitalSelectQuery]
  
      selectQuery.setLimit(limit)
      
      val random = new Random(1000L)
      
      var skipped = 0
      
      
      
      
      while(offset >= 0) {
        
  
        selectQuery.setOffset(offset)
        
        val rl = vitalService.query(selectQuery)
        
        var c = 0
        
        var l = new java.util.ArrayList[(String, Array[Byte])]()
        
        var limitReached = false
        
        for( g <- rl) {
          
          if(maxDocs > 0 && total >= maxDocs) {
            
            limitReached = true
            
          } else {
          
            total = total + 1
            
            var accept = true
            
            if(percent < 100D) {
            
              if(random.nextDouble() * 100D > percent) {
                accept = false
                skipped = skipped + 1
              }
            }
              
            if(accept) {
              
          	  var bytes : Array[Byte] = null;
              
              //dump to output
              if(uriOnly || expandObjects) {
                
                val uriRef = new URIReference()
                uriRef.setURI(URIGenerator.generateURI(null, classOf[URIReference], true))
                uriRef.setProperty("uRIRef", URIProperty.withString(g.getURI()));
                
                l.add((g.getURI, VitalSigns.get.encodeBlock(Arrays.asList(uriRef))))
                
                //just dump to output
              } else {
                
            	  l.add((g.getURI, VitalSigns.get.encodeBlock(Arrays.asList(g))))
                
              }
              
            }
              
          }
          
          
        }
        
        if(l.size() > 0) {
          
          if(uriOnly || !expandObjects) {
            
        	  output = output.union(sc.parallelize(l))
            
          } else {
            
            inputURIsList = inputURIsList.union(sc.parallelize(l))
            
          }
          
          
        }
        
        if( !limitReached && c == limit ) {
          offset += limit
        } else {
          offset = -1
        }
        
  
      }
      
      println("Total graph objects: " + total + ", skipped: " + skipped)
      
    } else {
      
      inputURIsList = sc.sequenceFile(urisListPath.toString(), classOf[Text], classOf[VitalBytesWritable]).map { pair =>
        
        //verify input list is a list of URI elements ?
        (pair._1.toString(), pair._2.get)
      }
      
//      inputBlockRDD = sc.sequenceFile(inputPath.toString(), classOf[Text], classOf[VitalBytesWritable]).map { pair =>
//            (pair._1.toString(), pair._2.get)
//          }
      //just load uris list from collection
      
    }
    
    
    //expand objects now
    if(inputURIsList != null) {
      
      output = inputURIsList.map { pair =>
        
        if(VitalSigns.get.getVitalService() == null) {
          
          if(profile != null) {
            VitalServiceFactory.setServiceProfile(profile)
          }
    
          VitalSigns.get.setVitalService( VitalServiceFactory.getVitalService() )
          
        }
        
        val inputObjects = VitalSigns.get().decodeBlock(pair._2, 0, pair._2.length)
            
        val vitalBlock = new VitalBlock(inputObjects)
        
        //model is not intialized but has feature exctractor that will fetch all the data
        aspenModel.getFeatureExtraction.composeBlock(vitalBlock)
        
        (vitalBlock.getMainObject.getURI, VitalSigns.get.encodeBlock(vitalBlock.toList()))
        
      }
      
    }
    
    if(outputPath.startsWith("name:")) {
      
    	println("persisting as named RDD: " + outputRDDName)
      
      this.namedRdds.update(outputRDDName, output)
      
    } else {
      
    	println("writing results to sequence file: " + outputPath)
    	
    	val hadoopOutput = output.map( pair =>
    	  (new Text(pair._1), new VitalBytesWritable(pair._2))
      )
    			
    	hadoopOutput.saveAsSequenceFile(outputPath)
      
      
      
    }
    
    
  }
  
  override def subvalidate(sc: SparkContext, config: Config) : SparkJobValidation = {
    
    val outputValue = config.getString(outputOption.getLongOpt)
    
    if(!skipNamedRDDValidation && outputValue.startsWith("name:")) {
      
      try{
    	  if(this.namedRdds == null) {
    	  } 
      } catch { case ex: NullPointerException => {
    	  return new SparkJobInvalid("Cannot use named RDD output - no spark job context")
        
      }}
        
      
    }
    
    val queryPath = getOptionalString(config, queryOption)
    val urisListPath = getOptionalString(config, urisListOption)
    
    if(queryPath != null && urisListPath != null) return new SparkJobInvalid("query-file and uris-list are mutually exclusive")
    if(queryPath == null && urisListPath == null) return new SparkJobInvalid("no query-file nor uris-list, exactly one required")
    
    val uriOnly = config.getBoolean(uriOnlyOption.getLongOpt)
    
    if(urisListPath != null && uriOnly) {
      return new SparkJobInvalid("uris-list and uri-only as output does not make sense")
    }
    
    var percent = 100D
    val percentValue = getOptionalString(config, percentOption)
    if(percentValue != null) {
      percent = java.lang.Double.parseDouble(percentValue)
    }
    if(percent <= 0D || percent > 100D) {
      return new SparkJobInvalid("percent value must be in (0; 100] range: " + percent)
    }
    
    
    SparkJobValid
    
  }
  
}

