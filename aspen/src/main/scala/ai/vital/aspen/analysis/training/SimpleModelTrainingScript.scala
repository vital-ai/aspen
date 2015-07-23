package ai.vital.aspen.analysis.training

import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.BasicParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.ParseException
import org.apache.hadoop.fs.Path
import ai.vital.aspen.job.AbstractJob
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.commons.io.IOUtils
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.vitalsigns.VitalSigns
import ai.vital.aspen.groovy.modelmanager.ModelTaxonomySetter
import ai.vital.aspen.groovy.featureextraction.FeatureExtraction
import ai.vital.aspen.groovy.featureextraction.FeatureData
import java.util.HashMap
import scala.collection.JavaConversions._
import ai.vital.predictmodel.TextFeature
import ai.vital.aspen.groovy.featureextraction.TextFeatureData
import ai.vital.predictmodel.BinaryFeature
import ai.vital.aspen.groovy.featureextraction.BinaryFeatureData
import ai.vital.predictmodel.CategoricalFeature
import ai.vital.aspen.groovy.featureextraction.CategoricalFeatureData
import ai.vital.predictmodel.DateFeature
import ai.vital.aspen.groovy.featureextraction.DateFeatureData
import ai.vital.predictmodel.DateTimeFeature
import ai.vital.aspen.groovy.featureextraction.DateTimeFeatureData
import ai.vital.predictmodel.GeoLocationFeature
import ai.vital.aspen.groovy.featureextraction.GeoLocationFeatureData
import ai.vital.predictmodel.NumericalFeature
import ai.vital.aspen.groovy.featureextraction.NumericalFeatureData
import ai.vital.predictmodel.OrdinalFeature
import ai.vital.aspen.groovy.featureextraction.OrdinalFeatureData
import ai.vital.predictmodel.StringFeature
import ai.vital.aspen.groovy.featureextraction.StringFeatureData
import ai.vital.predictmodel.URIFeature
import ai.vital.aspen.groovy.featureextraction.URIFeatureData
import ai.vital.predictmodel.WordFeature
import ai.vital.aspen.groovy.featureextraction.WordFeatureData
import ai.vital.aspen.groovy.featureextraction.Dictionary

/**
 * A script that generates simple models - models that do not require training
 */
object SimpleModelTrainingScript {

  val modelBuilderOption = new Option("b", "model-builder", true, "model builder file")
  modelBuilderOption.setRequired(true)

  val outputOption = new Option("mod", "model", true, "output model path (directory)")
  outputOption.setRequired(true)

  val overwriteOption = new Option("ow", "overwrite", false, "overwrite model if exists")
  overwriteOption.setRequired(false)
  
  val profileOption = new Option("prof", "profile", true, "optional vitalservice profile option")
  profileOption.setRequired(false)

  def main(args: Array[String]): Unit = {

    val parser = new BasicParser();

    val options = new Options()
      .addOption(modelBuilderOption)
      .addOption(outputOption)
      .addOption(overwriteOption)

    if (args.length == 0) {
      val hf = new HelpFormatter()
      hf.printHelp("simple-model-training", options)
      return
    }

    var cmd: CommandLine = null

    try {
      cmd = parser.parse(options, args);
    } catch {
      case ex: ParseException => {
        System.err.println(ex.getLocalizedMessage());
        return
      }
    }

    val builderPath = new Path(cmd.getOptionValue(modelBuilderOption.getLongOpt))
    
    val outputModelPath = new Path(cmd.getOptionValue(outputOption.getLongOpt))
    
    val overwrite = cmd.hasOption(overwriteOption.getLongOpt);
    
    val serviceProfile = cmd.getOptionValue(profileOption.getLongOpt)
    
    println("builder path: " + builderPath)
    println("output model path: " + outputModelPath)
    println("overwrite if exists: " + overwrite)
    
    println("service profile: " + serviceProfile)
    
    val creator = ModelTrainingJob.getModelCreator()
    
    val builderFS = FileSystem.get(builderPath.toUri(), new Configuration())
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
    
    
    val modelFS = FileSystem.get(outputModelPath.toUri(), new Configuration())
    
    if( modelFS.exists(outputModelPath)) {
      if(!overwrite) throw new RuntimeException("Output model path already exists: " + outputModelPath)
      println("Deleteing existing model: " + outputModelPath)
      modelFS.delete(outputModelPath, true)
    }
    
    val aspenModel = creator.createModel(builderBytes)
    
    if(serviceProfile != null) {
      VitalServiceFactory.setServiceProfile(serviceProfile)
    }
    
    VitalSigns.get.setVitalService(VitalServiceFactory.getVitalService)
 
    
    ModelTaxonomySetter.loadTaxonomies(aspenModel, null)
    
    val fd = new HashMap[String, FeatureData]()
    for ( f <- aspenModel.getModelConfig.getFeatures ) {
      if(f.isInstanceOf[BinaryFeature]) {
        fd.put(f.getName, new BinaryFeatureData())
      } else if(f.isInstanceOf[CategoricalFeature]) {
    	  fd.put(f.getName, new CategoricalFeatureData())
      } else if(f.isInstanceOf[DateFeature]) {
    	  fd.put(f.getName, new DateFeatureData())
      } else if(f.isInstanceOf[DateTimeFeature]) {
    	  fd.put(f.getName, new DateTimeFeatureData())
      } else if(f.isInstanceOf[GeoLocationFeature]) {
    	  fd.put(f.getName, new GeoLocationFeatureData())
      } else if(f.isInstanceOf[NumericalFeature]) {
    	  fd.put(f.getName, new NumericalFeatureData())
      } else if(f.isInstanceOf[OrdinalFeature]) {
    	  fd.put(f.getName, new OrdinalFeatureData())
      } else if(f.isInstanceOf[StringFeature]) {
    	  fd.put(f.getName, new StringFeatureData())
      } else if(f.isInstanceOf[TextFeature]) {
        val tfd = new TextFeatureData()
        tfd.setDictionary(new Dictionary(new HashMap[String, Integer]()))
        fd.put(f.getName, tfd)
      } else if(f.isInstanceOf[URIFeature]) {
        fd.put(f.getName, new URIFeatureData())
      } else if(f.isInstanceOf[WordFeature]) {
      	fd.put(f.getName, new WordFeatureData())
      }
    }
    
    aspenModel.setFeaturesData(fd)
    
    var asJar = false
    
    if(outputModelPath.toString().endsWith(".jar") || outputModelPath.toString().endsWith(".zip")) {
      asJar = true
    }
    
    aspenModel.persist(modelFS, outputModelPath, asJar)
    
    println("Model persisted " + outputModelPath)

  }

}