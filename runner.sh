project_path="/home/armeria/Work/courses/db_calc/Lab1-1183710106"
java_source_path=$project_path"/src/main/java"

run_all=0
run_single=0
init=0
while getopts "a:s:i" arg; do
    case $arg in
    a)
        run_all=$OPTARG
        ;;
    s)
        run_single=$OPTARG
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

printf "Plan to run: "
if [ "$run_all" -ge 1 ] || [ "$run_single" -eq 1 ]; then
    printf "Sampler "
fi
if [ "$run_all" -ge 2 ] || [ "$run_single" -eq 2 ]; then
    printf "Filter "
fi
if [ "$run_all" -ge 1 ] || [ "$run_single" -eq 3 ]; then
    printf "MinMax "
fi
printf "\n"

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

if [ "$run_all" -ge 1 ] || [ "$run_single" -eq 1 ]; then
    # Prepare and run 1st run_all]: sampler
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

if [ "$run_all" -ge 2 ] || [ "$run_single" -eq 2 ]; then
    # Prepare and run 2st run_all]: filter
    cd $java_source_path || exit
    hdfs dfs -rm -r /user/armeria/bdclab1/filter_output
    hadoop jar main.jar Filter /user/armeria/bdclab1/sampler_output /user/armeria/bdclab1/filter_output
    cd $project_path || exit
    rm -rf ./filter_output
    hadoop fs -copyToLocal /user/armeria/bdclab1/filter_output .
    zip -q results_filter.zip ./filter_output
fi

if [ "$run_all" -ge 3 ] || [ "$run_single" -eq 3 ]; then
    # Prepare and run 3rd run_all]: minmax
    cd $java_source_path || exit
    hdfs dfs -rm -r /user/armeria/bdclab1/minmax_output
    hadoop jar main.jar MinMax /user/armeria/bdclab1/filter_output /user/armeria/bdclab1/minmax_output
    cd $project_path || exit
    rm -rf ./minmax_output
    hadoop fs -copyToLocal /user/armeria/bdclab1/minmax_output .
    zip -q results_minmax.zip ./minmax_output
fi
