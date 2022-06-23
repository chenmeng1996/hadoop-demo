#!/bin/bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/openjdk-11.jdk/Contents/Home
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar

hadoop com.sun.tools.javac.Main WordCountV1.java
jar cf wc.jar WordCount*.class

hadoop jar wc.jar WordCount /user/joe/wordcount/input /user/joe/wordcount/output
# show output
hadoop fs -cat /user/joe/wordcount/output/part-r-00000