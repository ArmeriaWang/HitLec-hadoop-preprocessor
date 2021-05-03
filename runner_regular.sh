# 项目根目录
project_path="/home/armeria/Work/courses/db_calc/Lab1-1183710106"

# .java源代码目录
java_source_path=$project_path"/src/main/java"

# HDFS目录
bdclab1_hpath="/user/armeria/bdclab1"
bdclab1_hpath_regular=$bdclab1_hpath"/regular"
bdclab1_hpath_effective=$bdclab1_hpath"/effective"

# Hadoop临时输出目录
hadoop_tmp_out_path="/home/armeria/Applications/hadoop-3.3.0/tmp"

# data.txt目录
data_path=$java_source_path/../resources/data.txt


echo_usage() {
    echo "Usage: runner_regular.sh [-i] (-a ROUNDS) | (-s ROUND)"
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

if [ $init -eq 1 ]; then
    cd $java_source_path || exit
    stop-dfs.sh
    rm -rf $hadoop_tmp_out_path
    hdfs namenode -format
    start-dfs.sh
    hdfs dfs -mkdir -p $bdclab1_hpath/input
    hdfs dfs -mkdir -p $bdclab1_hpath_regular
    hdfs dfs -mkdir -p $bdclab1_hpath_effective
    hadoop fs -copyFromLocal $data_path $bdclab1_hpath/input
fi

cd $project_path || exit
echo "Synchronizing from git..."
git pull
mvn clean
mvn validate

cd $java_source_path || exit
# Compile .java files
rm -r ./classes
hadoop com.sun.tools.javac.Main -d ./classes ./common/*.java ./effective/*.java ./regular/*.java

# Pack as main.jar
cd ./classes || exit
jar cf main.jar ./*
mv ./main.jar ../
cd ..

if [ "$run_all" -ge 1 ] || [ "$run_single" -eq 1 ]; then
    # Prepare and run 1st: sampler
    echo "Round 1 :: Sample - start!"
    hdfs dfs -rm -r $bdclab1_hpath_regular/sampler_output
    hadoop jar main.jar regular.Sampler $bdclab1_hpath/input $bdclab1_hpath_regular/sampler_output 0.005
    # hdfs dfs -cat $bdclab1_hpath_regular/output/part-r-00000 | head -n 20
    cd $project_path || exit
    rm -rf ./sampler_output
    hadoop fs -copyToLocal $bdclab1_hpath_regular/sampler_output .
    if [ ! -d "sampler_output" ]; then
        echo "\033[31mRound 1 :: Sample - failed!\033[0m"
        exit
    fi
    echo "\033[32mRound 1 :: Sample - success!\033[0m"
    # mv /home/armeria/debug* ./sampler_output
    # mv /home/armeria/real_samples* ./sampler_output
    # cd ./sampler_output
    # zip -q results_0.zip part-r-00000 debug_info_0.txt real_samples_0.txt
fi

if [ "$run_all" -ge 2 ] || [ "$run_single" -eq 2 ]; then
    # Prepare and run 2st: filter
    echo "Round 2 :: Filter - start!"
    cd $java_source_path || exit
    hdfs dfs -rm -r $bdclab1_hpath_regular/filter_output
    hadoop jar main.jar regular.Filter $bdclab1_hpath_regular/sampler_output $bdclab1_hpath_regular/filter_output
    cd $project_path || exit
    rm -rf ./filter_output
    hadoop fs -copyToLocal $bdclab1_hpath_regular/filter_output .
    # zip -q results_filter.zip ./filter_output
    if [ ! -d "filter_output" ]; then
        echo "\033[31mRound 2 :: Filter - failed!\033[0m"
        exit
    fi
    echo "\033[32mRound2 :: Filter - success!\033[0m"
fi

if [ "$run_all" -ge 3 ] || [ "$run_single" -eq 3 ]; then
    # Prepare and run 3rd: minmax
    echo "Round 3 :: Minmax - start!"
    cd $java_source_path || exit
    hdfs dfs -rm -r $bdclab1_hpath_regular/minmax_output
    hadoop jar main.jar regular.MinMax $bdclab1_hpath_regular/filter_output $bdclab1_hpath_regular/minmax_output
    cd $project_path || exit
    rm -rf ./minmax_output
    hadoop fs -copyToLocal $bdclab1_hpath_regular/minmax_output .
    if [ ! -d "minmax_output" ]; then
        echo "\033[31mRound 3 :: MinMax - failed!\033[0m"
        exit
    fi
    # zip -q results_minmax.zip ./minmax_output
    echo "\033[32mRound 3 :: MinMax - success!\033[0m"
fi

if [ "$run_all" -ge 4 ] || [ "$run_single" -eq 4 ]; then
    # Prepare and run 4th: normalize
    echo "Round 4 :: Normalize - start!"
    cd $java_source_path || exit
    hdfs dfs -rm -r $bdclab1_hpath_regular/normalize_output
    hdfs dfs -cp $bdclab1_hpath_regular/minmax_output/part-r-00000 $bdclab1_hpath_regular/minmax_output/minmax.txt
    hadoop jar main.jar regular.Normalizer $bdclab1_hpath_regular/filter_output $bdclab1_hpath_regular/normalize_output $bdclab1_hpath_regular/minmax_output/minmax.txt
    cd $project_path || exit
    rm -rf ./normalize_output
    hadoop fs -copyToLocal $bdclab1_hpath_regular/normalize_output .
    if [ ! -d "normalize_output" ]; then
        echo "\033[31mRound 4 :: Normalize - failed!\033[0m"
        exit
    fi
    # zip -q results_normalize.zip ./normalize_output
    echo "\033[32Round 4 :: Normalize - success!\033[0m"
fi

if [ "$run_all" -ge 5 ] || [ "$run_single" -eq 5 ]; then
    # Prepare and run 4th: Fill
    echo "Round 5 :: Fill - start!"
    cd $java_source_path || exit
    hdfs dfs -rm -r $bdclab1_hpath_regular/fill_output
    hadoop jar main.jar regular.Filler $bdclab1_hpath_regular/normalize_output $bdclab1_hpath_regular/fill_output
    cd $project_path || exit
    rm -rf ./fill_output
    hadoop fs -copyToLocal $bdclab1_hpath_regular/fill_output .
    if [ ! -d "normalize_output" ]; then
        echo "\033[31mRound 5 :: Fill - failed!\033[0m"
        exit
    fi
    # mv /home/armeria/debug* ./fill_output
    # zip -q results_normalize.zip ./normalize_output
    echo "\033[32mRound 5 :: Fill - success!\033[0m"
fi

echo "\033[32mPlan success!\033[0m"
