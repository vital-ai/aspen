package ai.vital.aspen.data

class DatasetDowngradeJob {}

object DatasetDowngradeJob extends DataDowngradeUpgradeBase {

  def getJobClassName(): String = {
    classOf[DatasetDowngradeJob].getCanonicalName
  }
  
  def getLabel() : String = {"downgrade"}
  
  def getJobName() : String = { "dataset-downgrade" }
  
  def isUpgradeNotDowngrade() : Boolean = { false }
  
  def main(args: Array[String]): Unit = {
    
    _mainImpl(args)
     
  }
  
}