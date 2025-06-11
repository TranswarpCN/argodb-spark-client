# ArgoDB Spark Client 项目

## 项目简介

Holodesk Spark Client 是一个专为 Transwarp Holodesk 数据库设计的 Spark 客户端连接器，主要用于解决 Scala 版本兼容性问题，并提供高性能的数据访问能力。该项目基于 [holodeskcore](http://172.16.1.41/ze.zhang/holodeskcore/commits/WARP-106872-8.31) 开发。

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Scala Version](https://img.shields.io/badge/scala-2.12/2.13-brightgreen.svg)](https://www.scala-lang.org/)

## 主要功能

✅ **Scala 版本兼容性支持**  
- 支持 Scala 2.12 和 2.13 版本
- 解决不同 Scala 版本间的兼容性问题

🚀 **高性能数据访问**  
- 优化的序列化/反序列化机制
- 支持批量数据读写操作

🔍 **查询优化**  
- 列裁剪 (Column Pruning)
- 谓词下推 (Filter Pushdown)
- 分区裁剪 (Partition Pruning)

💾 **多数据源支持**  
- 支持连接 Transwarp Holodesk 数据库
- 提供统一的数据访问接口

## 快速开始

### 依赖配置

在您的 `build.sbt` 文件中添加以下依赖：

```scala
libraryDependencies += "io.transwarp" %% "argodb-spark-client" % "1.0.0"
```

### 基本使用示例

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Holodesk Example")
  .config("spark.sql.extensions", "io.transwarp.holodesk.spark.ArgodbSource")
  .getOrCreate()

// 读取 Holodesk 表数据
val df = spark.read
  .format("holodesk")
  .option("dbtablename", "your_table_name")
  .load()

df.show()
```

## 高级配置

### 连接参数

| 参数名 | 描述 | 默认值 |
|--------|------|--------|
| `dbtablename` | Holodesk 表名 | 必填 |
| `dialect` | 数据库方言 (ORACLE/DB2/TD) | "ORACLE" |
| `enable.column.pruner` | 启用列裁剪 | true |
| `enable.filter.pushdown` | 启用谓词下推 | true |
| `enable.partition.prune` | 启用分区裁剪 | true |

### 性能调优

```scala
spark.conf.set("spark.holodesk.batch.size", "10000")  // 设置批量读取大小
spark.conf.set("spark.holodesk.serde.buffer", "64MB") // 序列化缓冲区大小
```

## 构建项目

使用以下命令构建项目：

```bash
./build/mvn -DskipTests clean package
```

## 贡献指南

我们欢迎任何形式的贡献！请遵循以下步骤：

1. Fork 本项目
2. 创建您的特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交您的更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 提交 Pull Request

## 许可证

本项目采用 [Apache License 2.0](LICENSE) 开源协议。

## 联系方式

如有任何问题或建议，请通过以下方式联系我们：

- 邮箱：support@transwarp.io
- 问题追踪：[GitHub Issues](https://github.com/TranswarpCN/argodb-spark-client/issues)

---

© 2025 Transwarp Inc. 保留所有权利。
