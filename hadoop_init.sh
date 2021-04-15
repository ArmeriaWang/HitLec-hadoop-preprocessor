cd /home/armeria/Work/courses/db_calc/Lab1-1183710106/src/main/java
if [ $1 -eq 1 ]
then
    stop-dfs.sh
    rm -rf /home/armeria/Applications/hadoop-3.3.0/tmp
    hdfs namenode -format
    start-dfs.sh
    hdfs dfs -mkdir -p /user/armeria/bdclab1/input
    hadoop fs -copyFromLocal ../resources/data.txt /user/armeria/bdclab1/input
fi
hadoop com.sun.tools.javac.Main -d ./ ./*.java
jar cf sampler.jar *.class
hdfs dfs -rm -r /user/armeria/bdclab1/output
hadoop jar sampler.jar Sampler /user/armeria/bdclab1/ /user/armeria/bdclab1/output
