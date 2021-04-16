project_path="/home/armeria/Work/courses/db_calc/Lab1-1183710106"
java_source_path=$project_path"/src/main/java"
bdclab1_hpath="/user/armeria/bdclab1"

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
        echo "Usage: runner.sh [-i] [-a ROUNDS] | [-s ROUND]"
        echo "      1 Sample"
        echo "      2 Filter"
        echo "      3 MinMax"
        echo "      4 Normalize"
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
if [ "$run_all" -ge 3 ] || [ "$run_single" -eq 3 ]; then
    printf "MinMax "
fi
if [ "$run_all" -ge 4 ] || [ "$run_single" -eq 4 ]; then
    printf "Normalize "
fi
printf "\n"

cd $java_source_path || exit
if [ $init -eq 1 ]; then
    stop-dfs.sh
    rm -rf /home/armeria/Applications/hadoop-3.3.0/tmp
    hdfs namenode -format
    start-dfs.sh
    hdfs dfs -mkdir -p $bdclab1_hpath/input
    hadoop fs -copyFromLocal ../resources/data.txt $bdclab1_hpath/input
fi
git pull

# Compile .java files
hadoop com.sun.tools.javac.Main -d ./ ./*.java

# Pack as main.jar
jar cf main.jar *.class

if [ "$run_all" -ge 1 ] || [ "$run_single" -eq 1 ]; then
    # Prepare and run 1st]: sampler
    hdfs dfs -rm -r $bdclab1_hpath/sampler_output
    hadoop jar main.jar Sampler $bdclab1_hpath/input $bdclab1_hpath/sampler_output
    # hdfs dfs -cat $bdclab1_hpath/output/part-r-00000 | head -n 20
    cd $project_path || exit
    rm -rf ./sampler_output
    hadoop fs -copyToLocal $bdclab1_hpath/sampler_output .
    echo "Sample finished"
# mv /home/armeria/debug* ./sampler_output
# mv /home/armeria/real_samples* ./sampler_output
# cd ./sampler_output
# zip -q results_0.zip part-r-00000 debug_info_0.txt real_samples_0.txt
fi

if [ "$run_all" -ge 2 ] || [ "$run_single" -eq 2 ]; then
    # Prepare and run 2st]: filter
    cd $java_source_path || exit
    hdfs dfs -rm -r $bdclab1_hpath/filter_output
    hadoop jar main.jar Filter $bdclab1_hpath/sampler_output $bdclab1_hpath/filter_output
    cd $project_path || exit
    rm -rf ./filter_output
    hadoop fs -copyToLocal $bdclab1_hpath/filter_output .
    zip -q results_filter.zip ./filter_output
    echo "Filter finished"
fi

if [ "$run_all" -ge 3 ] || [ "$run_single" -eq 3 ]; then
    # Prepare and run 3rd]: minmax
    cd $java_source_path || exit
    hdfs dfs -rm -r $bdclab1_hpath/minmax_output
    hadoop jar main.jar MinMax $bdclab1_hpath/filter_output $bdclab1_hpath/minmax_output
    cd $project_path || exit
    rm -rf ./minmax_output
    hadoop fs -copyToLocal $bdclab1_hpath/minmax_output .
    zip -q results_minmax.zip ./minmax_output
    echo "MinMax finished"
fi

if [ "$run_all" -ge 4 ] || [ "$run_single" -eq 4 ]; then
    # Prepare and run 4th]: normalize
    cd $java_source_path || exit
    hdfs dfs -rm -r $bdclab1_hpath/normalize_output
    hdfs dfs -cp $bdclab1_hpath/minmax_output/part-r-00000 $bdclab1_hpath/minmax_output/minmax.txt
    hadoop jar main.jar Normalizer $bdclab1_hpath/filter_output $bdclab1_hpath/normalize_output $bdclab1_hpath/minmax_output/minmax.txt
    cd $project_path || exit
    rm -rf ./normalize_output
    hadoop fs -copyToLocal $bdclab1_hpath/normalize_output .
    zip -q results_normalize.zip ./normalize_output
    echo "Normalize finished"
fi