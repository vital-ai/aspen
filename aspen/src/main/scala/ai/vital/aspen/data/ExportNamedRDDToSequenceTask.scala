package ai.vital.aspen.data

import ai.vital.aspen.groovy.task.AbstractTask
import java.util.Collections
import java.util.List
import java.util.ArrayList
import java.util.Arrays
import ai.vital.aspen.groovy.convert.tasks.CheckPathTask

class ExportNamedRDDToSequenceTask(inputName: String, outputPath: String, context : java.util.Map[String, Object]) extends AbstractTask(context) {


  def getOutputParams(): List[String] = {
    new ArrayList[String]()
  }

  def getRequiredParams(): List[String] = {
    Arrays.asList(CheckPathTask.PATH_EXISTS_PREFIX + inputName)
  }
  
  
  
  def getInputName() : String = {
	  inputName;
  }
  
  def getOutputPath() : String = {
	  outputPath
  }
  
}