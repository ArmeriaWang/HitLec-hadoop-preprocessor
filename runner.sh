project_path="/home/armeria/Work/courses/db_calc/Lab1-1183710106"
java_source_path=$project_path"/src/main/java"
bdclab1_hpath="/user/armeria/bdclab1"

echo_usage() {
    echo "Usage: runner.sh [-i] (-a ROUNDS) | (-s ROUND)"
    echo "        1 Sample"
    echo "        2 Filter"
    echo "        3 MinMax"
    echo "        4 Normalize"
    echo "        5 Fill"
}

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
        echo_usage
        exit
        ;;
    esac
done
if [ "$init" -eq 0 ] && [ "$run_all" -eq 0 ] && [ "$run_single" -eq 0 ]; then
    echo_usage
    exit
fi

printf "Plan to run: "
if [ $init -eq 1 ]; then
    printf "Init "
fi
if [ "$run_all" -ge 1 ] || [ "$run_single" -eq 1 ]; then
    printf "Sample "
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
if [ "$run_all" -ge 5 ] || [ "$run_single" -eq 5 ]; then
    printf "Fill "
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

echo "Synchronizing from git..."
git pull

# Compile .java files
hadoop com.sun.tools.javac.Main -d ./ ./*.java

# Pack as main.jar
jar cf main.jar *.class

if [ "$run_all" -ge 1 ] || [ "$run_single" -eq 1 ]; then
    # Prepare and run 1st: sampler
    echo "Sample start"
    hdfs dfs -rm -r $bdclab1_hpath/sampler_output
    hadoop jar main.jar Sampler $bdclab1_hpath/input $bdclab1_hpath/sampler_output 0.005
    # hdfs dfs -cat $bdclab1_hpath/output/part-r-00000 | head -n 20
    cd $project_path || exit
    rm -rf ./sampler_output
    hadoop fs -copyToLocal $bdclab1_hpath/sampler_output .
    if [ ! -d "sampler_output" ]; then
        echo "\033[31mSample failed\033[0m"
        exit
    fi
    echo "\033[32mSample success\033[0m"
    # mv /home/armeria/debug* ./sampler_output
    # mv /home/armeria/real_samples* ./sampler_output
    # cd ./sampler_output
    # zip -q results_0.zip part-r-00000 debug_info_0.txt real_samples_0.txt
fi

if [ "$run_all" -ge 2 ] || [ "$run_single" -eq 2 ]; then
    # Prepare and run 2st: filter
    echo "Filter start"
    cd $java_source_path || exit
    hdfs dfs -rm -r $bdclab1_hpath/filter_output
    hadoop jar main.jar Filter $bdclab1_hpath/sampler_output $bdclab1_hpath/filter_output
    cd $project_path || exit
    rm -rf ./filter_output
    hadoop fs -copyToLocal $bdclab1_hpath/filter_output .
    # zip -q results_filter.zip ./filter_output
    if [ ! -d "filter_output" ]; then
        echo "\033[31mFilter failed\033[0m"
        exit
    fi
    echo "\033[32mFilter success\033[0m"
fi

if [ "$run_all" -ge 3 ] || [ "$run_single" -eq 3 ]; then
    # Prepare and run 3rd: minmax
    echo "Minmax start"
    cd $java_source_path || exit
    hdfs dfs -rm -r $bdclab1_hpath/minmax_output
    hadoop jar main.jar MinMax $bdclab1_hpath/filter_output $bdclab1_hpath/minmax_output
    cd $project_path || exit
    rm -rf ./minmax_output
    hadoop fs -copyToLocal $bdclab1_hpath/minmax_output .
    if [ ! -d "minmax_output" ]; then
        echo "\033[31mMinMax failed\033[0m"
        exit
    fi
    # zip -q results_minmax.zip ./minmax_output
    echo "\033[32mMinMax success\033[0m"
fi

if [ "$run_all" -ge 4 ] || [ "$run_single" -eq 4 ]; then
    # Prepare and run 4th: normalize
    echo "Normalize start"
    cd $java_source_path || exit
    hdfs dfs -rm -r $bdclab1_hpath/normalize_output
    hdfs dfs -cp $bdclab1_hpath/minmax_output/part-r-00000 $bdclab1_hpath/minmax_output/minmax.txt
    hadoop jar main.jar Normalizer $bdclab1_hpath/filter_output $bdclab1_hpath/normalize_output $bdclab1_hpath/minmax_output/minmax.txt
    cd $project_path || exit
    rm -rf ./normalize_output
    hadoop fs -copyToLocal $bdclab1_hpath/normalize_output .
    if [ ! -d "normalize_output" ]; then
        echo "\033[31mNormalize failed\033[0m"
        exit
    fi
    # zip -q results_normalize.zip ./normalize_output
    echo "\033[32mNormalize success\033[0m"
fi

if [ "$run_all" -ge 5 ] || [ "$run_single" -eq 5 ]; then
    # Prepare and run 4th: Fill
    echo "Fill start"
    cd $java_source_path || exit
    hdfs dfs -rm -r $bdclab1_hpath/fill_output
    hadoop jar main.jar Filler $bdclab1_hpath/normalize_output $bdclab1_hpath/fill_output
    cd $project_path || exit
    rm -rf ./fill_output
    hadoop fs -copyToLocal $bdclab1_hpath/fill_output .
    if [ ! -d "normalize_output" ]; then
        echo "\033[31mFill failed\033[0m"
        exit
    fi
    # zip -q results_normalize.zip ./normalize_output
    echo "\033[32mFill success\033[0m"
fi

echo "\033[32mPlan success\033[0m"
