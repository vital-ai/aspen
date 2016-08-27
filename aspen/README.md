aspen
=====

Aspen: AI on Spark

### Building modes:
1. Default
 
        mvn clean package [install]

2. Aspen module jar for vitalprime etc

        mvn -f aspen-module-pom.xml clean package

3. Aspen jobs jar for spark - contains all jobs with dependencies including domain jars
  domain jars artifacts should be listed under <dependencies> in aspen-jobs-pom.xml

        mvn -f aspen-jobs-pom.xml clean package


### Cluster notes

Important notes when deploying a job jar in a cluster consisting of spark-jobserver + spark master + spark worker(s)

*  **Jimfs** <br/>
 It is necessary to add ```jimfs-1.0.jar``` and ```guava-18.0.jar``` jars (copies located in$VITAL\_HOME/vitalsigns/lib) to spark master and workers boot classpaths. The simplest way to do so it set/update ```SPARK_CLASSPATH``` in $SPARK\_HOME/conf/spark-env.sh file

        SPARK_CLASSPATH=<jars.dir>/jimfs-1.0.jar:<jars.dir>/guava-18.0.jar