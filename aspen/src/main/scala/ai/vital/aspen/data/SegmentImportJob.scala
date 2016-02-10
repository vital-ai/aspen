package ai.vital.aspen.data

import ai.vital.aspen.job.AbstractJob
import org.apache.commons.cli.Options
import org.apache.commons.cli.Option
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import ai.vital.aspen.util.SetOnceHashMap
import com.typesafe.config.ConfigList
import java.util.Arrays
import spark.jobserver.SparkJobValidation
import spark.jobserver.SparkJobValid
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.sql.service.VitalServiceSql
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import ai.vital.sql.model.VitalSignsToSqlBridge
import ai.vital.sql.model.VitalSignsToSqlBridge._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.DataFrame
import java.util.ArrayList
import org.apache.spark.sql.Row
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.rdd.RDD
import ai.vital.vitalsigns.VitalSigns
import org.apache.commons.csv.CSVFormat
import java.io.StringReader
import ai.vital.sql.services.ToCSVProviderImpl
import java.util.HashMap
import ai.vital.aspen.groovy.data.SegmentImportProcedure
import ai.vital.aspen.job.TasksHandler

class SegmentImportJob {}

object SegmentImportJob extends AbstractJob {
  
  val inputOption  = new Option("i", "input", true, "input vital sql .csv[.gz] location")
  inputOption.setRequired(true)
  
  val segmentIDOption   = new Option("sid", "segmentID", true, "segmentID option")
  segmentIDOption.setRequired(true)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite output if exists")
  overwriteOption.setRequired(false)
  
  def getJobClassName(): String = {
    classOf[SegmentImportJob].getCanonicalName
  }

  def getJobName(): String = {
    "segment-import"
  }

  def getOptions(): Options = {
    addJobServerOptions(
      new Options().
        addOption(masterOption).
        addOption(inputOption).
        addOption(segmentIDOption).
        addOption(profileOption).
        addOption(profileConfigOption).
//        addOption(serviceKeyOption).
        addOption(overwriteOption)
    )
  }
  
  val customSchema = StructType(java.util.Arrays.asList(
        StructField( COLUMN_ID, LongType, true),
        StructField( COLUMN_URI, StringType, true),
        StructField( COLUMN_NAME, StringType, true),
        StructField( COLUMN_VITALTYPE, StringType, true),
        StructField( COLUMN_EXTERNAL, BooleanType, true),
        StructField( COLUMN_VALUE_BOOLEAN, BooleanType, true),
        StructField( COLUMN_VALUE_BOOLEAN_MULTIVALUE, BooleanType, true),
        StructField( COLUMN_VALUE_DATE, LongType, true),
        StructField( COLUMN_VALUE_DATE_MULTIVALUE, LongType, true),
        StructField( COLUMN_VALUE_DOUBLE, DoubleType, true),
        StructField( COLUMN_VALUE_DOUBLE_MULTIVALUE, DoubleType, true),
        StructField( COLUMN_VALUE_FLOAT, FloatType, true),
        StructField( COLUMN_VALUE_FLOAT_MULTIVALUE, FloatType, true),
        StructField( COLUMN_VALUE_GEOLOCATION, StringType, true),
        StructField( COLUMN_VALUE_GEOLOCATION_MULTIVALUE, StringType, true),
        StructField( COLUMN_VALUE_INTEGER, IntegerType, true),
        StructField( COLUMN_VALUE_INTEGER_MULTIVALUE, IntegerType, true),
        StructField( COLUMN_VALUE_LONG, LongType, true),
        StructField( COLUMN_VALUE_LONG_MULTIVALUE, LongType, true),
        StructField( COLUMN_VALUE_OTHER, StringType, true),
        StructField( COLUMN_VALUE_OTHER_MULTIVALUE, StringType, true),
        StructField( COLUMN_VALUE_STRING, StringType, true),
        StructField( COLUMN_VALUE_STRING_MULTIVALUE, StringType, true),
        StructField( COLUMN_VALUE_TRUTH, ShortType, true),
        StructField( COLUMN_VALUE_TRUTH_MULTIVALUE, ShortType, true),
        StructField( COLUMN_VALUE_FULL_TEXT, StringType, true),
        StructField( COLUMN_VALUE_URI, StringType, true),
        StructField( COLUMN_VALUE_URI_MULTIVALUE, StringType, true)
    ))
  
  
  def runJob(sc: SparkContext, jobConfig: Config): Any = {
    
    
    val globalContext = new SetOnceHashMap()
    
    val ic = jobConfig.getValue(inputOption.getLongOpt)
    
    var inputPaths : java.util.List[String] = null
    
    if(ic.isInstanceOf[ConfigList]) {
      
      throw new RuntimeException("single input allowed")
//      inputPaths = jobConfig.getStringList(inputOption.getLongOpt);
      
    } else {
      
      inputPaths = Arrays.asList(jobConfig.getString(inputOption.getLongOpt))
      
    }
    
    val service = openVitalService()
    
    
    val segmentID = jobConfig.getString(segmentIDOption.getLongOpt)
    
    println("Input: " + inputPaths)
    println("SegmentID: " + segmentID)
    
    val overwrite = getBooleanOption(jobConfig, overwriteOption)
    
    println("Overwrite ? " + overwrite)
    
    val segmentImportProcedure = new SegmentImportProcedure(inputPaths, segmentID, overwrite, globalContext)
 
    val tasks  = segmentImportProcedure.generateTasks();
    
    val handler = new TasksHandler()
    
    handler.handleTasksList(this, tasks)

    service.close()
    
    println("DONE")
    
  }
  
  def readDataFrame(hiveContext : HiveContext, schema : StructType, path : String) : DataFrame = {
    
    val reader = hiveContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("nullValue", "")
    .option("treatEmptyValuesAsNulls", "true")
    .schema(customSchema)
    .option("inferSchema", "false") // Automatically infer data types

    reader.load(path);
  
  }
  
  def convertBlockRDDToDataFrame(hiveContext : HiveContext, schema : StructType, rdd : RDD[(String, Array[Byte])] ) : DataFrame = {
    
    val rowRDD = rdd.flatMap {  encoded =>
      
      val l = new ArrayList[Row]()
      
      for( g <- VitalSigns.get.decodeBlock(encoded._2, 0, encoded._2.length) ) {
        
        for( csvLine <- g.toCSV(false) ) {
          
          val reader = new StringReader(csvLine)
          
          val csvParser = CSVFormat.DEFAULT.parse(reader);
          
          for( csvRecord <- csvParser) {
            
            //
            val rawValues = new ArrayList[Object]()
            
            var i = 0 
            
            while( i < SegmentImportJob.customSchema.fields.size ) {
              
              val column = SegmentImportJob.customSchema.fields(i)
              val dt = column.dataType
              
              val s = csvRecord.get(i)
              
              var v : Object = null;
              
              if(s.isEmpty()) {
                
              } else {
                
                if(dt == StringType) {
                  v = s
                } else if(dt == BooleanType) {
                  v = new java.lang.Boolean(java.lang.Boolean.parseBoolean(s))
                } else if(dt == DoubleType) {
                  v = new java.lang.Double(java.lang.Double.parseDouble(s))
                } else if(dt == FloatType) {
                  v = new java.lang.Float(java.lang.Float.parseFloat(s))                  
                } else if(dt == IntegerType) {
                  v = new java.lang.Integer(java.lang.Integer.parseInt(s))
                } else if(dt == LongType) {
                  v = new java.lang.Long(java.lang.Long.parseLong(s))
                //convert based on schema type
                } else if(dt == ShortType) {
                  v = new java.lang.Short(java.lang.Short.parseShort(s))
                } else throw new RuntimeException("Unhandled schema column type: " + dt)
              }
              
              rawValues.add(v)
              
              i = i + 1
              
            }
            
            l.add( Row(rawValues : _*) )
            
          }
          
          csvParser.close()
          
        }
        
      }
      
      l
      
    }
    
    hiveContext.createDataFrame(rowRDD, schema)
    
  }
  
  def csvRecordToRowMap(record : java.util.List[String]) : java.util.Map[String, Object] = {
    
    val m = new HashMap[String, Object]
    
    var i = 0 ;
    
    while (i < record.size() ) {
      
      var s = record.get(i)
      
      val column = SegmentImportJob.customSchema.fields(i)
      
      val dt = column.dataType
              
      var v : Object = null;
              
      if(s.isEmpty()) {
                
      } else {
                
        if(dt == StringType) {
          v = s
        } else if(dt == BooleanType) {
          v = new java.lang.Boolean(java.lang.Boolean.parseBoolean(s))
        } else if(dt == DoubleType) {
          v = new java.lang.Double(java.lang.Double.parseDouble(s))
        } else if(dt == FloatType) {
          v = new java.lang.Float(java.lang.Float.parseFloat(s))                  
        } else if(dt == IntegerType) {
          v = new java.lang.Integer(java.lang.Integer.parseInt(s))
        } else if(dt == LongType) {
          v = new java.lang.Long(java.lang.Long.parseLong(s))
        //convert based on schema type
        } else if(dt == ShortType) {
          v = new java.lang.Short(java.lang.Short.parseShort(s))
        } else throw new RuntimeException("Unhandled schema column type: " + dt)
        
      }
      
      if(v != null) {
        
    	  m.put(column.name, v)
    	  
      }
              
      i = i + 1
      
    }
    
    
    m
    
  }

  override def subvalidate(sc : SparkContext, config : Config) : SparkJobValidation = {
 
    SparkJobValid
    
    
  }
  
  def main(args: Array[String]): Unit = {
    
    _mainImpl(args)
     
  }
}