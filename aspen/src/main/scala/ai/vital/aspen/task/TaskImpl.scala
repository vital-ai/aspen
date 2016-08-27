package ai.vital.aspen.task

import ai.vital.aspen.groovy.task.AbstractTask
import org.apache.spark.SparkContext

abstract class TaskImpl[T <: AbstractTask](sc : SparkContext, task: T) {
  
  def checkDependencies() : Unit
 
  def execute() : Unit
  
  def ex(m : String) : Unit =  { throw new RuntimeException(m)}
  
}