package ai.vital.aspen.data


class DatasetUpgradeJob {}

object DatasetUpgradeJob extends DataDowngradeUpgradeBase {
  
  def getJobClassName(): String = {
    classOf[DatasetUpgradeJob].getCanonicalName
  }
  
  def getLabel() : String = {"upgrade"}
  
  def getJobName() : String = { "dataset-upgrade" }
  
  def isUpgradeNotDowngrade() : Boolean = { true }
  
  def main(args: Array[String]): Unit = {
    
    _mainImpl(args)
     
  }
  
}