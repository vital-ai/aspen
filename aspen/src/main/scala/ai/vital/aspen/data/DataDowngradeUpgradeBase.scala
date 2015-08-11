package ai.vital.aspen.data

import ai.vital.aspen.job.AbstractJob
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import ai.vital.aspen.util.SetOnceHashMap
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import ai.vital.query.querybuilder.VitalBuilder
import org.apache.commons.io.IOUtils
import ai.vital.aspen.job.TasksHandler
import ai.vital.aspen.job.TasksHandler
import ai.vital.vitalservice.ServiceOperations
import ai.vital.vitalservice.UpgradeOptions
import ai.vital.vitalservice.DowngradeOptions
import ai.vital.vitalservice.BaseDowngradeUpgradeOptions
import java.util.ArrayList
import scala.collection.JavaConversions._

trait DataDowngradeUpgradeBase extends AbstractJob {

  val inputOption  = new Option("i", "input", true, "overrides path in a builder, input RDD[(String, Array[Byte])], either named RDD name (name:<name>) or <path> (no prefix), where path is a .vital.seq or .vital[.gz] file")
  inputOption.setRequired(false)
  
  val outputOption  = new Option("o", "output", true, "overrides path in a builder, output RDD[(String, Array[Byte])], either named RDD name (name:<name>) or <path> (no prefix), where path is a .vital.seq or .vital[.gz] file")
  outputOption.setRequired(false)
  
  val overwriteOption = new Option("ow", "overwrite", false, "overwrite output file if exists")
  overwriteOption.setRequired(false)

  val domainOntologiesOption = new Option("do", "domain-ontology", true, "comma-separated list of domain ontologies to load");
  domainOntologiesOption.setRequired(false)
  
  def getLabel() : String
  
  def isUpgradeNotDowngrade() : Boolean
  
  val builderOption = new Option("b", "builder", true, getLabel() + " builder file");
  builderOption.setRequired(false)
      
  val owlFileOption = new Option("owlfile", "owl-file", true, "overrides name in builder, older owl file path option")
  owlFileOption.setRequired(false)
  
  val owlDirectoryOption = new Option("owldirectory", "owl-directory", true, "ovverdides path in builder, older owl files directory")
  owlDirectoryOption.setRequired(false)
  
  def getOptions(): Options = {
    addJobServerOptions(
        new Options()
        .addOption(inputOption)
        .addOption(outputOption)
        .addOption(masterOption)
        .addOption(overwriteOption)
        .addOption(builderOption)
        .addOption(owlFileOption)
        .addOption(owlDirectoryOption)
        .addOption(domainOntologiesOption)
    )
  }
  
  def runJob(sc: SparkContext, jobConfig: Config): Any = {

    var inputPath = getOptionalString(jobConfig, inputOption.getLongOpt)
    val builderPath = getOptionalString(jobConfig, builderOption.getLongOpt)
    var outputPath = getOptionalString(jobConfig, outputOption.getLongOpt)
    var owlFile = getOptionalString(jobConfig, owlFileOption.getLongOpt)
    var owlDirectory = getOptionalString(jobConfig, owlDirectoryOption.getLongOpt)
    val overwrite = getBooleanOption(jobConfig, overwriteOption)
    
    val domainOntologiesParam = getOptionalString(jobConfig, domainOntologiesOption)
    
    println("dataset-" + getLabel())
    
    println("Input path: " + inputPath)
    println("Output path: " + outputPath)
    println("Builder path: " + builderPath)
    println("OWL File: " + owlFile)
    println("OWL Directory: " + owlDirectory)
    println("Overwrite: " + overwrite)
    
    val globalContext = new SetOnceHashMap
    
    var ops : ServiceOperations = null 
    
    val domainJarsList = new ArrayList[String]
    
    var builderContents : String = null
    
    if(builderPath != null) {
      
      val builderPathObj = new Path(builderPath)
      
      val builderFS = FileSystem.get(builderPathObj.toUri(), hadoopConfiguration);
      
      if(!builderFS.exists(builderPathObj)) throw new Exception("Builder file not found: " + builderPathObj.toString())
      
      if(!builderFS.isFile(builderPathObj)) throw new Exception("Builder path is not a file: " + builderPathObj.toString())
      
      val builderIS = builderFS.open(builderPathObj)
      builderContents = IOUtils.toString(builderIS, "UTF-8")
      IOUtils.closeQuietly(builderIS)
      
      var builder = new VitalBuilder()
      
      ops = builder.queryString(builderContents).toService()

      builder = null
      
      var bop : BaseDowngradeUpgradeOptions = null;
      
      if(isUpgradeNotDowngrade()) {
        
    	  val uop = ops.getUpgradeOptions();
        
        if(uop == null) throw new Exception("No upgrade options found");
        
        if(ops.getDowngradeOptions() != null) throw new Exception("Cannot use both downgrade and upgrade options")
        
        for( um <- uop.getUpgradeMappings ) {
          um.setClosure(um.getClosure.dehydrate()) 
        }
        
        bop = uop
        
      } else {
        
        val dop = ops.getDowngradeOptions();
        
        if(dop == null) throw new Exception("No downgrade options set"); 
        
        if(ops.getUpgradeOptions() != null) throw new Exception("Cannot use both downgrade and upgrade options")
        
        for( dm <- dop.getDowngradeMappings ) {
          dm.setClosure(dm.getClosure.dehydrate())
        }
        
        bop = dop
        
      }
      
      if(bop.getOldOntologiesDirectory == null) throw new Exception("Builder file does not specify oldOntologiesDirectory")
      if(bop.getOldOntologyFileName == null) throw new Exception("Builder file does not specify oldOntologyFileName")

      if(bop.getDestinationSegment != null || bop.getSourceSegment != null) throw new Exception("Aspen does not support segments data migration")
      
      if(inputPath == null) {
        inputPath = bop.getSourcePath
        println("Input path from builder: " + inputPath)
        if(inputPath == null) throw new Exception("No input path in builder nor as cli param")
      }
      
      if(outputPath == null) {
        outputPath = bop.getDestinationPath
        println("Output path from builder: " + outputPath);
        if(outputPath == null) throw new Exception("No output path in builder nor as cli param")
      }
      
      if(owlFile == null) {
        owlFile = bop.getOldOntologyFileName
        println("OWL filename from builder: " + owlFile);
        if(owlFile == null) throw new Exception("No owl file name in builder nor as cli param")
      }
      
      if(owlDirectory == null) {
        owlDirectory = bop.getOldOntologiesDirectory
        println("OWL directory from builder: " + owlDirectory)
        if(owlDirectory == null) throw new Exception("No owl directory in builder nor as cli param")
      }
      
      if(domainOntologiesParam == null) {
    	  if( bop.getDomainJars != null ) {  
          println("Using domain jars list from builder:  " + bop.getDomainJars);
          domainJarsList.addAll(bop.getDomainJars)
        }
      } else {
        for(j <- domainOntologiesParam.split(",") ) {
          if(!j.trim().isEmpty()) {
        	  domainJarsList.add(j.trim());
          }
        }
        println("Using cli domain jars list:  " + domainJarsList);
      }
      
      
    } else {
      
      builderContents = ""

      if(isUpgradeNotDowngrade()) {
        
        builderContents = "UPGRADE { }"
        
      } else {
        
    	  builderContents = "DOWNGRADE { }"
        
      }
      
      var builder = new VitalBuilder()
      
      ops = builder.queryString(builderContents).toService()

      builder = null
      
      ops = new ServiceOperations()
      
      if(inputPath == null) throw new Exception("input path is required when no builder file is specified")
      
      if(outputPath == null) throw new Exception("output path is required when no builder file is specified")
      
      
      if(owlDirectory == null) throw new Exception("owlDirectory is required when no builder file is specified")
      if(owlFile == null) throw new Exception("owlFile is required when no builder file is specified")
      
      for(j <- domainOntologiesParam.split(",") ) {
        if(!j.trim().isEmpty()) {
          domainJarsList.add(j.trim());
        }
      }
      
      println("Using cli domain jars list:  " + domainJarsList);
      
    }
    
    
    loadDynamicDomainJarsList(domainJarsList)
    
    
    val parentLoader = LoaderSingleton.initParent(owlDirectory, owlFile, hadoopConfiguration);

    
    LoaderSingleton.init(parentLoader.getMainDomainBytes, parentLoader.getOtherDomainBytes, builderContents)
    
    if(isUpgradeNotDowngrade()) {
      LoaderSingleton.OLD_DOMAIN_INPUT = true
    } else {
      LoaderSingleton.OLD_DOMAIN_OUTPUT = true
    }
    
    
    
    val procedure = new DowngradeUpgradeProcedureSteps(globalContext, inputPath, outputPath, ops, builderContents, overwrite);
    
    val handler = new TasksHandler()
    
    val tasks = procedure.generateTasks()
    
    handler.handleTasksList(this, tasks)
    
    var stats = globalContext.get(DowngradeUpgradeProcedureTask.DATASET_STATS)
    
    if(stats == null) stats = "(no dataset stats)"
    
    println(stats)
    
    println("DONE")
    
    return stats
    
  }
  
  
}