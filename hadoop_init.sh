cd /home/armeria/Work/courses/db_calc/Lab1-1183710106/src/main/java
if [ "$1" -eq 1 ]
then
    stop-dfs.sh
    rm -rf /home/armeria/Applications/hadoop-3.3.0/tmp
    hdfs namenode -format
    start-dfs.sh
    hdfs dfs -mkdir -p /user/armeria/bdclab1/input
    hadoop fs -copyFromLocal ../resources/data.txt /user/armeria/bdclab1/input
fi
git pull
hadoop com.sun.tools.javac.Main -d ./ ./*.java
jar cf sampler.jar *.class
hdfs dfs -rm -r /user/armeria/bdclab1/output
hadoop jar sampler.jar Sampler /user/armeria/bdclab1/input /user/armeria/bdclab1/output
hdfs dfs -cat /user/armeria/bdclab1/output/part-r-00000 | head -n 20
rm -rf ../../../output
hadoop fs -get /user/armeria/bdclab1/output ../../../
mv /home/armeria/debug* ../../../output
mv /home/armeria/read_samples* ../../../output
zip -q results_0.zip part-r-00000 debug_info_0.txt read_samples_0.txt