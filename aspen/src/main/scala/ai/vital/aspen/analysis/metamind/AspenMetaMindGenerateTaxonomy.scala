package ai.vital.aspen.analysis.metamind

import java.io.File
import groovy.json.JsonSlurper
import scala.collection.JavaConversions._
import java.util.Map
import java.text.Normalizer
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringEscapeUtils
import ai.vital.vitalsigns.model.VITAL_Category

object AspenMetaMindGenerateTaxonomy {

  def main(args: Array[String]) : Unit = {
    
    if(args.length != 3) {
      System.err.println("usage: <mode> <input_classes_json> <output_dsl_taxonomy_part_file>");
      System.err.println(" mode is one of [ inline, taxonomy_file ]");
      return
    }
    
    val URI_BASE = "http://vital.ai/vital.ai/app/" + classOf[VITAL_Category].getSimpleName() + "/metamind_"
    
    var mode = args(0)
    
    if( ! ( mode.equals("inline") || mode.equals("taxonomy_file") ) ) {
      System.err.println("Mode invalid: " + mode)
      return
    }
    
    val inputFile = new File(args(1))
    
    val outputFile = new File(args(2))
    
    println("Input classes json: " + inputFile.getAbsolutePath)
    println("Output taxonomy snippet: " + outputFile.getAbsolutePath)
    
    println("Generating taxonomy ");
    
    val catObj = new JsonSlurper().parse(inputFile).asInstanceOf[Map[String, Object]];
    
    val classes = catObj.get("classes").asInstanceOf[java.util.List[String]]
    
    println("Classes count: " + classes.size())
    
    var s = ""
    
    if(mode.equals("inline")) {
    
      s = s"""
  TAXONOMY {
    
    value provides: 'metamind-taxonomy'
      
    CATEGORY {
      value name: 'Root Category'
      value uri: '${URI_BASE}ROOT'
"""

      var index = 0
      for(cls <- classes) {
      
        val uri = URI_BASE + index
      
        s += s"""

      CATEGORY {
        value name: "${StringEscapeUtils.escapeJava(cls)}"
        value uri: '$uri'
      }
"""
      
        index = index + 1
        
      }
    
      s += """
    }
  }
"""
    } else if(mode.equals("taxonomy_file")) {
      
      var index = 0
      
      s = s"""
# metamind flat taxonomy

+ ${URI_BASE}ROOT  "MetaMind Taxonomy Root"

 
"""
      
//      # example 20news group taxonomy, 1 root and 20 children
//
//+ http://vital.ai/20news/Category/Taxonomy
//
//++ http://vital.ai/20news/Category/alt.atheism
//...
      
      for(cls <- classes) {
        
        val uri = URI_BASE + index
      
        s += s"""
++ $uri "$cls"
"""
        index = index + 1
        
      }
      
    }

    
    FileUtils.writeStringToFile(outputFile, s, "UTF-8")
    
  }
  
  def slugify(inputX : String) : String = {
    
    if (inputX == null) {
      return "";
    }

    var input = inputX.trim();

//    for (Entry<Object, Object> e : replacements.entrySet()) {
//      input = input.replace((String) e.getKey(), (String) e.getValue());
//    }

    input = Normalizer.normalize(input, Normalizer.Form.NFD)
        .replaceAll("[^\\p{ASCII}]", "")
        .replaceAll("[^\\w+]", "-")
        .replaceAll("\\s+", "-")
        .replaceAll("[-]+", "-")
        .replaceAll("^-", "")
        .replaceAll("-$", "");

//    if (getLowerCase()) {
      input = input.toLowerCase();
//    }

    return input;
    
  }
  
}