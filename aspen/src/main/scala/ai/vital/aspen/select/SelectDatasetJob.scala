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
import ai.vital.aspen.groovy.select.SelectDatasetProcedure
import ai.vital.aspen.util.SetOnceHashMap
import ai.vital.aspen.job.TasksHandler

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
    .addOption(profileConfigOption)
    .addOption(serviceKeyOption)
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
    
    val builderPath = jobConfig.getString(modelBuilderOption.getLongOpt)
    
    var percent = 100
    val percentValue = getOptionalString(jobConfig, percentOption)
    if(percentValue != null) {
    	percent = java.lang.Integer.parseInt(percentValue)
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
    
    val vitalService = openVitalService()
    
    val globalContext = new SetOnceHashMap()
    
    val procedure = new SelectDatasetProcedure(builderPath, outputPath, queryPathParam, urisListPath, limit, percent, maxDocs, overwrite, uriOnly, globalContext)

    val tasks = procedure.generateTasks()
    
    val handler = new TasksHandler()
    
    handler.handleTasksList(this, tasks)

    println("DONE")
    
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
    
    var percent = 100
    val percentValue = getOptionalString(config, percentOption)
    if(percentValue != null) {
      percent = java.lang.Integer.parseInt(percentValue)
    }
    if(percent <= 0 || percent > 100) {
      return new SparkJobInvalid("percent value must be in (0; 100] range: " + percent)
    }
    
    
    SparkJobValid
    
  }
  
}

