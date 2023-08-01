# Apache-Spark-Studies
Projetos realizados nos cursos de Apache Spark

Variáveis de ambiente do log4j:
%SPARK_HOME%/config/spark-defaults.config

spark.driver.extraJavaOptions -Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=hello-spark
