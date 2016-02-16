package ai.vital.aspen.thriftserver

import ai.vital.aspen.job.AbstractJob
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.sql.hive.HiveContext
import scala.collection.JavaConversions._

class VitalThriftServer {}

object VitalThriftServer extends AbstractJob {
  
  val cacheTablesOption = new Option("ct", "cache-tables", false, "cache tables on startup")
  cacheTablesOption.setRequired(false)
  
  val databaseOption = new Option("db", "database", true, "database to cache, required with cache tables")
  databaseOption.setRequired(false)
  
  def getJobClassName(): String = {
    classOf[VitalThriftServer].getCanonicalName    
  }

  def getJobName(): String = {
    "vital-thrift-server"
  }

  def getOptions(): Options = {
    addJobServerOptions(
      new Options()
        .addOption(masterOption)
        .addOption(cacheTablesOption)
        .addOption(databaseOption)
    )
  }
  
  def runJob(sc: SparkContext, jobConfig: Config): Any = {
   
    
    //precache s1 table
    val cacheTables = getBooleanOption(jobConfig, cacheTablesOption)
    
    println("Cache Tables: " + cacheTables)
    
    val databaseName = getOptionalString(jobConfig, databaseOption)
    
    if(cacheTables.booleanValue()) {
      
      if(databaseName == null) {
        throw new RuntimeException("database param is required when caching tables")
      }
      
    }
    val hiveContext = new HiveContext(sc)
    
    println("Starting thrift server...")
    
    HiveThriftServer2.startWithContext(hiveContext)
    		
    println("Thrift server started.")
    
    if(cacheTables.booleanValue()) {
      
      println("Listing tables...")
    
      val tables = hiveContext.sql("show tables in `" + databaseName + "`").collectAsList()
      
      
      println("tables count: " + tables.size())
      
      for(r <- tables) {
        
        val tname = r.getAs[String](0)
        
        println("Caching table " + tname + " ... ")
        
        val start = System.currentTimeMillis()
        
        hiveContext.cacheTable("`" + databaseName + "`.`" + tname + "`")
        
        println("Table " + tname + " cached, time: " + ( System.currentTimeMillis() - start ) + "ms")
             
      }
      
      println ("All tables cached")
      
    } else {
      
      println ("Tables caching disabled")
    }
    
    
    
  }
 
  
  def main(args: Array[String]): Unit = {
    
    _mainImpl(args)
     
  }
  
}