### 环境

```shell
$ uname -a
Linux 5.4.0-47-generic #51-Ubuntu SMP Fri Sep 4 19:50:52 UTC 2020 x86_64 x86_64 x86_64 GNU/Linux

$ java --version
java 11.0.10 2021-01-19 LTS
Java(TM) SE Runtime Environment 18.9 (build 11.0.10+8-LTS-162)
Java HotSpot(TM) 64-Bit Server VM 18.9 (build 11.0.10+8-LTS-162, mixed mode)

$ bin/hadoop version
Hadoop 3.3.0
Source code repository https://gitbox.apache.org/repos/asf/hadoop.git -r aa96f1871bfd858f9bac59cf2a81ec470da649af
Compiled by brahma on 2020-07-06T18:44Z
Compiled with protoc 3.7.1
From source with checksum 5dc29b802d6ccd77b262ef9d04d19c4
This command was run using hadoop-3.3.0/share/hadoop/common/hadoop-common-3.3.0.jar

$ mvn -v        
Apache Maven 3.8.1 (05c21c65bdfed0f71a2f2ada8b84da59348c4c5d)
Java version: 11.0.10, vendor: Oracle Corporation, runtime: /usr/lib/jvm/jdk-11.0.10
Default locale: en_US, platform encoding: UTF-8
OS name: "linux", version: "5.4.0-47-generic", arch: "amd64", family: "unix"

$ git --version
git version 2.25.1
```



### 编译和运行

进入项目根目录，编辑 `runner.sh` ，将最上方的路径变量更改为自己的路径。

一键编译运行：

```shell
$ ./runner.sh -i -a 5
```

### 未缩减 MapReduce 轮数的版本

两个辅助类

- `CareerWritable` 职业 枚举类，实现了 `WritableComparable` 接口。
- `ReviewWritable` 评价类，实现了 `Writable` 接口。

五个过程类

- `Sampler` 抽样，3个参数依次为 输入路径、输出路径和采样率。
- `Filter` 过滤，抛掉经纬度不在有效范围内的数据。
- `MinMax` 求最大 / 最小值，为归一化做准备。
- `Normalizer` 归一化，将 `Rating` 属性做 Min-Max 归一化。
- `Filler` 填充，用线性回归模型填充缺失的 `Rating` 值，用对应国家和职业的平均数填充缺失的 `UserIncome` 值。