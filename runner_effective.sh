# 项目根目录
project_path="/home/armeria/Work/courses/db_calc/Lab1-1183710106"

# .java源代码目录
java_source_path=$project_path"/src/main/java"

# HDFS目录
bdclab1_hpath="/user/armeria/bdclab1"
bdclab1_hpath_effective=$bdclab1_hpath"/effective"

# Hadoop临时输出目录
hadoop_tmp_out_path="/home/armeria/Applications/hadoop-3.3.0/tmp"

# data.txt目录
data_path=$java_source_path/../resources/data.txt

# 本地结果目录
local_results_path=$project_path/effective_output

echo_usage() {
    echo "Usage: runner_effective.sh [-i] (-a ROUNDS) | (-s ROUND)"
    echo "        1 Sample, Filter, MinMax"
    echo "        2 Normalize"
    echo "        3 Fill"
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
    printf "Sample Filter MinMax "
fi
if [ "$run_all" -ge 2 ] || [ "$run_single" -eq 2 ]; then
    printf "Normalize "
fi
if [ "$run_all" -ge 3 ] || [ "$run_single" -eq 3 ]; then
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

mkdir -p $local_results_path

if [ "$run_all" -ge 1 ] || [ "$run_single" -eq 1 ]; then
    # Prepare and run 1st: Sample Filter MinMax
    echo "Round 1 :: Sample Filter MinMax - start!"
    hdfs dfs -rm -r $bdclab1_hpath_effective/sample_filter_minmax_output
    hdfs dfs -mkdir $bdclab1_hpath_effective/minmax
    hadoop jar main.jar effective.SampleFilterMinMax $bdclab1_hpath/input  `# class name, input path` \
        $bdclab1_hpath_effective/sample_filter_minmax_output 0.005         `# output path, sample rate` \
        $bdclab1_hpath_effective/minmax/minmax.txt    `# path of minmax.txt`
    cd $local_results_path || exit
    rm -rf ./sample_filter_minmax_output
    hadoop fs -copyToLocal $bdclab1_hpath_effective/sample_filter_minmax_output .
    hadoop fs -copyToLocal $bdclab1_hpath_effective/minmax .
    if [ ! -d "sample_filter_minmax_output" ]; then
        echo "\033[31mRound 1 :: Sample Filter MinMax failed!\033[0m"
        exit
    fi
    echo "\033[32mRound 1 :: Sample Filter MinMax success!\033[0m"
fi

if [ "$run_all" -ge 2 ] || [ "$run_single" -eq 2 ]; then
    # Prepare and run 2st: Normalize Fill
    echo "Round 2 :: Normalize Fill - start!"
    cd $java_source_path || exit
    hdfs dfs -rm -r $bdclab1_hpath_effective/normalize_filler_output
    hadoop jar main.jar effective.NormalizeFiller `# class name`\
        $bdclab1_hpath_effective/sample_filter_minmax_output `# input path`\
        $bdclab1_hpath_effective/normalize_filler_output `# output path`\
        $bdclab1_hpath_effective/minmax/minmax.txt `# path of minmax.txt`
    cd $local_results_path || exit
    rm -rf ./normalize_filler_output
    hadoop fs -copyToLocal $bdclab1_hpath_effective/normalize_filler_output .
    # zip -q results_filter.zip ./filter_output
    if [ ! -d "normalize_filler_output" ]; then
        echo "\033[31mRound 2 :: Normalize Fill - failed!\033[0m"
        exit
    fi
    echo "\033[32mRound 2 :: Normalize Fill - success!\033[0m"
fi

echo "\033[32mPlan success\033[0m"
