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

object SegmentImportJob extends AbstractJob {
  
  val inputOption  = new Option("i", "input", true, "input vital sql .csv[.gz] location")
  inputOption.setRequired(true)
  
  val segmentIDOption   = new Option("sid", "segmentID", true, "segmentID option")
  segmentIDOption.setRequired(true)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite output if exists")
  overwriteOption.setRequired(false)
  
  def getJobClassName(): String = {
    classOf[DatasetImportJob].getCanonicalName
  }

  def getJobName(): String = {
    "dataset-import"
  }

  def getOptions(): Options = {
    addJobServerOptions(
      new Options().
        addOption(masterOption).
        addOption(inputOption).
        addOption(segmentIDOption).
        addOption(profileOption).
//        addOption(serviceKeyOption).
        addOption(overwriteOption)
    )
  }
  
  
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
    
    
    val segmentID = jobConfig.getString(segmentIDOption.getLongOpt)
    
    println("Input: " + inputPaths)
    println("SegmentID: " + segmentID)
    
    val overwrite = getBooleanOption(jobConfig, overwriteOption)
    
    println("Overwrite ? " + overwrite)

    def vitalService = VitalServiceFactory.openService(serviceKey, serviceProfile)
    
    if(!vitalService.isInstanceOf[VitalServiceSql]) throw new Exception("Expected instance of " + classOf[VitalServiceSql].getCanonicalName ) 
    
    val segment = vitalService.getSegment(segmentID)
    if(segment == null) throw new Exception("Segment with ID " + segmentID + " not found")
    
    val tableName = vitalService.asInstanceOf[VitalServiceSql].getSegmentTableName(segment)
    
    println("Table Name: " + tableName)
    
    val hiveContext = new HiveContext(sparkContext)
    

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
    //obtain segment table name from vital-sql
    

    val initDF = hiveContext.table(tableName)
    
    val newDF = readDataFrame(hiveContext, customSchema, inputPaths.get(0))
    
    var outputDF : DataFrame = null
    
    if(overwrite) {
      
      outputDF = newDF
      
    } else {
      
    	val g1 = initDF.map { r => ( r.getAs[String](COLUMN_URI), r) }.groupBy({ p => p._1})
			val g2 = newDF.map { r => ( r.getAs[String](COLUMN_URI), r ) }.groupBy({ p => p._1})
			
			val join = g1.fullOuterJoin(g2).map { pair =>
			
  			val oldProps = pair._2._1
			
	  		val newProps = pair._2._2
			
		  	if(newProps.size < 1) {
			  	oldProps.get
  			} else {
	  			newProps.get
		  	}
			
			}
			
			val outputRDD = join.flatMap { pair =>
			
			  val l = new ArrayList[Row]()
			
			  for( p <- pair ) {
				
				  l.add(p._2)
				
			  }
			
			  l
			
			}
    			
    	//very simple yet requires some driver memory to collect all uris in source segment
//    val newURIs = newDF.map { r => r.getAs[String](COLUMN_URI) }.distinct().collect()
//    val outputDF = initDF.filter(initDF.col(COLUMN_URI).isin(newURIs : _*).unary_!).unionAll(newDF)
    			
    	outputDF = hiveContext.createDataFrame(outputRDD, customSchema)
    	
    }
    

    val saveMode = SaveMode.Overwrite
    
    outputDF.write.mode(saveMode).saveAsTable(tableName)

    vitalService.close()
    
    println("DONE")
    
  }
  
  def readDataFrame(hiveContext : HiveContext, customSchema : StructType, path : String) : DataFrame = {
    
    val df = hiveContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("nullValue", "")
    .option("treatEmptyValuesAsNulls", "true")
    .schema(customSchema)
    .option("inferSchema", "false") // Automatically infer data types
    .load(path);
  
    df
      
  }
  
  override def subvalidate(sc : SparkContext, config : Config) : SparkJobValidation = {
 
    SparkJobValid
    
    
  }
  
  def main(args: Array[String]): Unit = {
    
    _mainImpl(args)
     
  }
}