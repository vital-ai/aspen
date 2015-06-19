aspen
=====

Aspen: AI on Spark


Building modes:
1. Default 
mvn clean package [install]

2. Aspen module jar for vitalprime etc
mvn -f aspen-module-pom.xml clean package

3. Aspen jobs jar for spark - contains all jobs with dependencies including domain jars
  domain jars artifacts should be listed under <dependencies> in aspen-jobs-pom.xml
mvn -f aspen-jobs-pom.xml clean package