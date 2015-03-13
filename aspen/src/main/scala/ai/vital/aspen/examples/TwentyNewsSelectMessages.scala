package ai.vital.aspen.examples

import ai.vital.aspen.job.AbstractJob
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.typesafe.config.ConfigException
import ai.vital.vitalservice.VitalService
import ai.vital.vitalservice.factory.Factory
import ai.vital.query.graphbuilder.GraphQueryBuilder
import ai.vital.vitalservice.query.graph.VitalSelectQuery
import scala.collection.JavaConversions._
import ai.vital.vitalsigns.model.GraphMatch
import ai.vital.vitalsigns.datatype.VitalURI
import ai.vital.vitalsigns.meta.GraphContext
import ai.vital.vitalsigns.model.container.GraphObjectsIterable
import ai.vital.hadoop.writable.VitalBytesWritable
import org.apache.spark.rdd.RDD
import ai.vital.vitalsigns.model.GraphObject
import scala.collection.mutable.MutableList
import ai.vital.vitalsigns.VitalSigns
import java.util.Arrays
import org.apache.hadoop.io.Text
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object TwentyNewsSelectMessages extends AbstractJob {
  
  def getJobName(): String = {
    return "twentynews select message job"
  }
  
  def getJobClassName(): String = {
    TwentyNewsSelectMessages.getClass.getCanonicalName
  }

  val outputOption = new Option("o", "output", true, "output RDD[(String,Array[Byte])], either RDD name or path:<path>, where path is a .vital.seq file")
  outputOption.setRequired(true)
  
  val segmentOption = new Option("s", "segment", true, "20news segment id")
  segmentOption.setRequired(true)
  
  val limitOption = new Option("l", "limit", true, "optional documents limit (page), default 1000")
  limitOption.setRequired(false)
  
  val maxOption = new Option("max", "maxDocs", true, "optional max documents limit (hardlimit)")
  maxOption.setRequired(false)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite output file if exists")
  overwriteOption.setRequired(false)
  
  def getOptions(): Options = {
    val options = new Options()
        .addOption(masterOption)
        .addOption(profileOption)
        .addOption(outputOption)
        .addOption(segmentOption)
        .addOption(limitOption)
        .addOption(maxOption)
        .addOption(overwriteOption)

    return options
  }
  
   def main(args: Array[String]): Unit = {
    
     _mainImpl(args)
     
   }
  
  def runJob(sc: SparkContext, jobConfig: Config): Any = {
    
    val segment = jobConfig.getString(segmentOption.getLongOpt)
    
    val outputPath = jobConfig.getString(outputOption.getLongOpt)
    
    val overwrite = jobConfig.getBoolean(overwriteOption.getLongOpt)
    
    println("segment: " + segment)
    println("output: " + outputPath)
    println("Overwrite ? " + overwrite)
    
    if(!outputPath.startsWith("path:")) {
        throw new RuntimeException("NamedRDD disabled, use path: prefixed output")
    } else {
      
      val outputBlockPath = new Path(outputPath.substring(5))
      
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
    
    var maxDocs = -1
    try {
      maxDocs = jobConfig.getInt(maxOption.getLongOpt)
    }catch{ case ex: Exception=>{} }
    
    println("limit: " + limit)
    println("maxDocs: " + maxDocs)
    
    if(profile != null) {
      println("Setting custom vitalservice profile: " + profile)
      Factory.setServiceProfile(profile)
    } else {
      println("Using default service profile...")
    }
    
    val vitalService = Factory.getVitalService
    
    val builder = new GraphQueryBuilder()
    
    var offset = 0
    
    var total = 0
    
    var output : RDD[(String, Array[Byte])] = sc.parallelize(Array[(String, Array[Byte])]())
    
    while(offset >= 0) {
      
      val selectQuery = builder.queryString(s"""
import org.example.twentynews.domain.*
import ai.vital.vitalservice.segment.VitalSegment

SELECT {

  value segments: [VitalSegment.withId('${segment}')]

  value limit: ${limit} 
  value offset: ${offset}  

//  source { bind { 'src' } }

  node_constraint { Message.class }

}
""").toQuery()

      val rl = vitalService.selectQuery(selectQuery.asInstanceOf[VitalSelectQuery])
      
      var c = 0
      
      val uris = new java.util.HashSet[String]()
      
      val urisList = new java.util.ArrayList[VitalURI]()
      
      
      for( g <- rl) {
        
        val gm = g.asInstanceOf[GraphMatch]
        
        val docURI = gm.getOverriddenMap.entrySet().iterator().next().getValue().toString()
        
        if(uris.add(docURI)) {
          urisList.add(VitalURI.withString(docURI))
        }
        
        c = c+1
        
      }
      
      val res = vitalService.get(urisList, GraphContext.ServiceWide, new java.util.ArrayList[GraphObjectsIterable]())
      
      var l = new java.util.ArrayList[(String, Array[Byte])]()
      
      var limitReached = false
      
      for( g <- res) {
        
        if(maxDocs > 0 && total >= maxDocs) {
          
          limitReached = true
          
        } else {
        
        	total = total + 1
        			
          val bytes = VitalSigns.get.encodeBlock(Arrays.asList(g))
          
     			l.add((g.getURI, bytes))
          
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
    
    println("Total graph objects: " + total)
    
    if(outputPath.startsWith("path:")) {
      
      println("writing results to sequence file")
      
      val hadoopOutput = output.map( pair =>
        (new Text(pair._1), new VitalBytesWritable(pair._2))
       )
     
      hadoopOutput.saveAsSequenceFile(outputPath.substring(5))
      
    } else {
      
      println("persisting as named RDD...")
      
//      this.namedRdds.update(outputPath, output)
      
    }
    
    
  }
}