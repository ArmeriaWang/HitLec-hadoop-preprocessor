project_path="/home/armeria/Work/courses/db_calc/Lab1-1183710106"
java_source_path=$project_path"/src/main/java"

round=0
init=0
while getopts "r:i" arg; do
    case $arg in
    r)
        round=$OPTARG
        ;;
    i)
        init=1
        ;;
    ?)
        echo "Usage: runner.sh [-i] -r ROUNDS"
        exit 1
        ;;
    esac
done

cd $java_source_path || exit
if [ $init -eq 1 ]; then
    stop-dfs.sh
    rm -rf /home/armeria/Applications/hadoop-3.3.0/tmp
    hdfs namenode -format
    start-dfs.sh
    hdfs dfs -mkdir -p /user/armeria/bdclab1/input
    hadoop fs -copyFromLocal ../resources/data.txt /user/armeria/bdclab1/input
fi
git pull

# Compile .java files
hadoop com.sun.tools.javac.Main -d ./ ./*.java

# Pack as main.jar
jar cf main.jar *.class

if [ "$round" -ge 1 ]; then
    # Prepare and run 1st round: sampler
    hdfs dfs -rm -r /user/armeria/bdclab1/sampler_output
    hadoop jar main.jar Sampler /user/armeria/bdclab1/input /user/armeria/bdclab1/sampler_output
    # hdfs dfs -cat /user/armeria/bdclab1/output/part-r-00000 | head -n 20
    cd $project_path || exit
    rm -rf ./sampler_output
    hadoop fs -copyToLocal /user/armeria/bdclab1/sampler_output .
# mv /home/armeria/debug* ./sampler_output
# mv /home/armeria/real_samples* ./sampler_output
# cd ./sampler_output
# zip -q results_0.zip part-r-00000 debug_info_0.txt real_samples_0.txt
fi

if [ "$round" -ge 2 ]; then
    # Prepare and run 2st round: filter
    cd $java_source_path || exit
    hdfs dfs -rm -r /user/armeria/bdclab1/filter_output
    hadoop jar main.jar Filter /user/armeria/bdclab1/sampler_output /user/armeria/bdclab1/filter_output
    cd $project_path || exit
    rm -rf ./filter_output
    hadoop fs -copyToLocal /user/armeria/bdclab1/filter_output .
    zip -q results_sam_fil.zip ./sampler_output ./filter_output
fi
