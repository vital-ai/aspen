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
import ai.vital.property.URIProperty
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
  
  val outputOption = new Option("o", "output", true, "output RDD[(String,Array[Byte])], either named RDD name (name:<name>) or <path> (no prefix), where path is a .vital.seq file")
  outputOption.setRequired(true)
  
  val queryOption = new Option("q", "query-file", true, "select query builder file")
  queryOption.setRequired(true)
  
  val limitOption = new Option("l", "limit", true, "optional documents limit (page size), default 1000")
  limitOption.setRequired(false)
  
  val percentOption = new Option("p", "percent", true, "optional output objects percent limit")
  percentOption.setRequired(false)
  
  val maxOption = new Option("max", "maxDocs", true, "optional max documents limit (hardlimit)")
  maxOption.setRequired(false)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite output file if exists")
  overwriteOption.setRequired(false)
  
  val uriOnlyOption = new Option("u", "uri-only", false, "only return uris, block bytes field will be null")
  
  def getOptions(): Options = {
    addJobServerOptions(
    new Options()
    .addOption(masterOption)
    .addOption(profileOption)
    .addOption(outputOption)
    .addOption(queryOption)
    .addOption(limitOption)
    .addOption(percentOption)
    .addOption(maxOption)
    .addOption(overwriteOption)
    .addOption(uriOnlyOption)
    )
  }

  def runJob(sc: SparkContext, jobConfig: Config): Any = {

    val queryPath = new Path(jobConfig.getString(queryOption.getLongOpt))
    
    val outputPath = jobConfig.getString(outputOption.getLongOpt)
    
    val overwrite = jobConfig.getBoolean(overwriteOption.getLongOpt)
    
    var percent = 100D
    try {
    	val percentValue = jobConfig.getString(percentOption.getOpt)
      percent = java.lang.Double.parseDouble(percentValue)
    } catch {
        case ex: ConfigException.Missing =>{}
    }
    
    val uriOnly = jobConfig.getBoolean(uriOnlyOption.getLongOpt)
    
    if(percent <= 0D || percent > 100D) {
        throw new RuntimeException("percent value must be in (0; 100] range: " + percent)
    }
    
    println("query builder path: " + queryPath)
    println("output: " + outputPath)
    println("Overwrite ? " + overwrite)
    println("URI only ? " + uriOnly)

    val queryFS = FileSystem.get(queryPath.toUri(), new Configuration())
    
    if(!queryFS.exists(queryPath)) throw new RuntimeException("Query builder file not found: " + queryPath.toString())
    
    val queryStream = queryFS.open(queryPath)
    val queryString = IOUtils.toString(queryStream, StandardCharsets.UTF_8.name())
    queryStream.close()
    
    var outputRDDName : String = null
    
    if(outputPath.startsWith("name:")) {
      outputRDDName = outputPath.substring("name:".length())
      println("output is a namedRDD: " + outputRDDName)
    }
    
    if(outputRDDName != null) {
      
    } else {
      
      val outputBlockPath = new Path(outputPath)
      
      val outpuBlockFS = FileSystem.get(outputBlockPath.toUri(), new Configuration())
      
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
    
    var profile : String = null
    try { 
      profile = jobConfig.getString(profileOption.getLongOpt) 
    } catch {
         case ex: ConfigException.Missing =>{}
    }
    
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
    
    val vitalService = VitalServiceFactory.getVitalService
    
    val builder = new VitalBuilder()
    
    var offset = 0
    
    var total = 0
    
    var output : RDD[(String, Array[Byte])] = sc.parallelize(Array[(String, Array[Byte])]())
    
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
            
            if( ! uriOnly ) {
              bytes = VitalSigns.get.encodeBlock(Arrays.asList(g))
            }
        			  
            l.add((g.getURI, bytes))
            
            
          }
            
        }
        
        
      }
      
      if(l.size() > 0) {
        
        output = output.union(sc.parallelize(l))
        
      }
      
      if( !limitReached && c == limit ) {
        offset += limit
      } else {
        offset = -1
      }
      

    }
    
    println("Total graph objects: " + total + ", skipped: " + skipped)
    
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
    
    if(outputValue.startsWith("name:")) {
      
      try{
    	  if(this.namedRdds == null) {
    	  } 
      } catch { case ex: NullPointerException => {
    	  return new SparkJobInvalid("Cannot use named RDD output - no spark job context")
        
      }}
        
      
    }
    
    SparkJobValid
    
  }
  
}

