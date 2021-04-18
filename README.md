### 未缩减 MapReduce 轮数的版本

两个辅助类

- `CareerWritable` 职业 枚举类，实现了 `WritableComparable` 接口。

- `ReviewWritable` 评价类，实现了 `Writable` 接口。

五个过程类

- `Sampler` 抽样，3个参数依次为 输入路径、输出路径和采样率。
- `Filter` 过滤，抛掉经纬度不在有效范围内的数据。
- `MinMax` 求最大 / 最小值，为归一化做准备。
- `Normalizer` 归一化，将`Rating` 属性做 Min-Max 归一化。
- `Filler` 填充，用线性回归模型填充缺失的 `Rating` 值，用平均数填充缺失的 `UserIncome` 值。