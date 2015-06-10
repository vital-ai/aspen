package ai.vital.aspen.examples

import org.apache.commons.cli.BasicParser
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.ParseException
import java.io.File
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import org.example.autompg.domain.AutoMpg
import ai.vital.vitalsigns.block.BlockCompactStringSerializer

object AutoMpgToVitalBlock {

	val NS = "http://vital.ai/autompg/"
  
  def main(args: Array[String]) : Unit = {
  
    
    val parser = new BasicParser();

    val inputOption = new Option("i", "input-file", true, "input autompg data file (auto-mpg.data)")
    inputOption.setRequired(true)
    
    val outputOption = new Option("o", "output-block", true, "output vital block file .vital[.gz] or .vital.seq")
    outputOption.setRequired(true)
      
    val overwriteOption = new Option("ow", "overwrite", false, "overwrite output file if exists")
    overwriteOption.setRequired(false)
      
    val options = new Options()
      .addOption(inputOption)
      .addOption(outputOption)
      .addOption(overwriteOption)
      
    if (args.length == 0) {
      val hf = new HelpFormatter()
      hf.printHelp(TwentyNewsToVitalBlock.getClass.getCanonicalName, options)
      return
    }


    var cmd: CommandLine = null
  
    try {
      cmd = parser.parse(options, args);
    } catch {
      case ex: ParseException => {
        error(ex.getLocalizedMessage());
        return
      }
    }
    
    val inputFile = new File(cmd.getOptionValue(inputOption.getLongOpt))
    val outputFile = new File(cmd.getOptionValue(outputOption.getLongOpt))
    val overwrite = cmd.hasOption(overwriteOption.getOpt)
    
    println("Input: " + inputFile.getAbsolutePath)
    println("Output: " + outputFile.getAbsolutePath)
    println("overwrite ? " + overwrite)
    
    if(outputFile.exists()) {
      
      if(!overwrite) {
        error("Output file already exists: " + outputFile.getAbsolutePath)
      }
      
      println("Overwriting existing file " + outputFile.getAbsolutePath)
      
    }
    
    val reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), StandardCharsets.UTF_8.name()))
    
    val writer = new BlockCompactStringSerializer(outputFile)
    
    var line = reader.readLine()
    
    var l = 0
    
    var c = 0
    
    while(line != null) {

      l = l + 1
      
      line = line.trim()
      
      if(line.isEmpty()) {
        
      } else {
        
        val dataNameCols = line.split("\t")
        if(dataNameCols.length != 2) error("Line " + l + ": expected two tsv columns")
        
        val data = dataNameCols(0)
        val name = dataNameCols(1).substring(1, dataNameCols(1).length() - 1)
        
        val dataCols = data.split("\\s+");
        
        if(dataCols.length != 8) error("Line " + l + ": expected 8 data columns") 
        
        
        if("?".equals( dataCols(3) ) ) {

          println("skipping missing horsepower line " + l + ": " + line)
          
        } else {
        
          val auto = new AutoMpg()
          
          
          auto.setProperty("mpg", java.lang.Double.parseDouble(dataCols(0)))
          auto.setProperty("cylinders", java.lang.Integer.parseInt(dataCols(1)))
          auto.setProperty("displacement", java.lang.Double.parseDouble(dataCols(2)))
          auto.setProperty("horsepower", java.lang.Double.parseDouble(dataCols(3)))
          auto.setProperty("weight", java.lang.Double.parseDouble(dataCols(4)))
          auto.setProperty("acceleration", java.lang.Double.parseDouble(dataCols(5)))
          auto.setProperty("modelYear", java.lang.Integer.parseInt(dataCols(6)))
          auto.setProperty("origin", java.lang.Integer.parseInt(dataCols(7)))
          auto.setProperty("name", name)
          
          c = c+1;
          auto.setURI(NS + c)
          
          writer.startBlock()
          
          writer.writeGraphObject(auto)
          
          writer.endBlock()
        
        }
      }
      
      line = reader.readLine()
      
      
      
      
    }
    
    
    reader.close()
    
    writer.close()
    
    println("records count: " + c)
    
  }
  
  def error(msg: String) : Unit = {
    System.err.println(msg)
    System.exit(1)
    
  }
  
}