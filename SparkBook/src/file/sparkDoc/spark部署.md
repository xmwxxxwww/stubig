# Spark的部署

```
Spark : 3.1.2
Hadoop : 3.2.1
```

##  基于windows的spark体验

### 解压

![img](https://cdn.nlark.com/yuque/0/2022/png/28792949/1652671151366-e24f01f2-f06f-47e3-b58b-51d3f227aeb7.png)

### 1.2 配置环境变量并启动

![img](https://cdn.nlark.com/yuque/0/2022/png/28792949/1652671275169-ce831af8-d374-487a-b7f8-e56d062ebf6d.png)

![img](https://cdn.nlark.com/yuque/0/2022/png/28792949/1652671327297-793dc656-a8ab-4774-a259-65cd4c939caf.png)

```
在配置好了环境变量之后
tip:
要求必须要配置SPARK_HOME这个变量，然后到其bin目录下启动spark-shell.cmd/spark-shell2.cmd
```

![img](https://cdn.nlark.com/yuque/0/2022/png/28792949/1652671626181-26d48ac1-a7d6-4932-a051-1e30724694a9.png)

### wordcount

```sh
scala> sc.textFile("H:/wc.txt")
.flatMap(_.split("\\s+"))
.map((_,1))
.reduceByKey(_+_)
.foreach(println)

(lizhi,1)
(wangsan,1)
(hello,5)
(you,1)
(lei,1)
(li,1)
(han,1)
(meimei,1)
```

![img](https://cdn.nlark.com/yuque/0/2022/png/28792949/1652671892088-c9526a3b-c1a5-4951-b9a1-07c8e72bf991.png)

## 基于Linux的Standalone模式的Spark安装

### standalone模式安装

```sh
##1. 创建目录
[root@hadoop ~]# mkdir -p /opt/apps ## 用于存放安装软件的目录
[root@hadoop ~]# mkdir -p /opt/software ## 用于存放安装包的目录


##2. 解压
[root@hadoop software]# tar -zxvf spark-3.1.2-bin-hadoop3.2.tgz  -C /opt/apps/
[root@hadoop software]# tar -zxvf scala-2.12.8.tgz -C /opt/apps/
[root@hadoop software]# tar -zxvf jdk-8u261-linux-x64.tar.gz -C /opt/apps/


##3. 配置环境变量
[root@hadoop scala-2.12.8]# vi /etc/profile

export JAVA_HOME=/opt/apps/jdk1.8.0_261
export SCALA_HOME=/opt/apps/scala-2.12.8
export SPARK_HOME=/opt/apps/spark-3.1.2-bin-hadoop3.2
export PATH=$PATH:$JAVA_HOME/bin:$SCALA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin
export CLASS_PATH=.:$JAVA_HOME/bin

[root@hadoop scala-2.12.8]# source /etc/profile

##4. 配置spark
[root@hadoop scala-2.12.8]# cd /opt/apps/spark-3.1.2-bin-hadoop3.2/conf/
##4.1 配置spark-env.sh
[root@hadoop conf]# mv spark-env.sh.template spark-env.sh
[root@hadoop conf]# vim spark-env.sh

export JAVA_HOME=/opt/apps/jdk1.8.0_261

##4.2 配置workers
[root@hadoop conf]# mv workers.template workers
[root@hadoop conf]# vim workers

hadoop

##5. 如果是全分布式，你还需要将conf目录中的所有的文件分发给其他的服务器节点
[root@hadoop conf]# scp -r /opt/apps/spark-3.1.2-bin-hadoop3.2/ root@hadoop01:$PWD

##6. 启动Spark集群
[root@hadoop sbin]# mv start-all.sh start-all-spark.sh
[root@hadoop sbin]# mv stop-all.sh stop-all-spark.sh
[root@hadoop sbin]# start-all-spark.sh ## 启动
[root@hadoop sbin]# stop-all-spark.sh ## 停止

##7. 查看webui
http://hadoop:8080
```

### 关于云服务器公网ip和内网ip的区别

![img](https://cdn.nlark.com/yuque/0/2022/png/28792949/1652681771632-b17f3b0f-9b8d-4c5d-ac59-5f826bc1eb8d.png)

### 集群结构

![img](https://cdn.nlark.com/yuque/0/2022/png/28792949/1652682190840-84c73c26-9bdc-43f0-9365-e70318c09187.png)

### spark-env.sh

```sh
# 使用spark-submit脚本的时候需要使用到以下的配置：
# ./bin/run-example or ./bin/spark-submit
# - HADOOP_CONF_DIR, to point Spark towards Hadoop configuration files
# - SPARK_LOCAL_IP, to set the IP address Spark binds to on this node
# - SPARK_PUBLIC_DNS, to set the public dns name of the driver program

# 配置执行器和驱动程序在服务器内部运行相关的配置
# - SPARK_LOCAL_IP, to set the IP address Spark binds to on this node
# - SPARK_PUBLIC_DNS, to set the public DNS name of the driver program
# - SPARK_LOCAL_DIRS, storage directories to use on this node for shuffle and RDD data
# - MESOS_NATIVE_JAVA_LIBRARY, to point to your libmesos.so if you use Mesos

# Spark使用Yarn模式的配置就在这个地方配置
# - SPARK_CONF_DIR, Alternate conf dir. (Default: ${SPARK_HOME}/conf)
# - HADOOP_CONF_DIR, to point Spark towards Hadoop configuration files
# - YARN_CONF_DIR, to point Spark towards YARN configuration files when you use YARN
# - SPARK_EXECUTOR_CORES, Number of cores for the executors (Default: 1).
# - SPARK_EXECUTOR_MEMORY, Memory per Executor (e.g. 1000M, 2G) (Default: 1G)
# - SPARK_DRIVER_MEMORY, Memory for Driver (e.g. 1000M, 2G) (Default: 1G)
export HADOOP_CONF_DIR=/opt/apps/hadoop-3.2.1/etc/hadoop
export YARN_CONF_DIR=/opt/apps/hadoop-3.2.1/etc/hadoop

# 配置使用standalone模式的spark集群的配置信息
# - SPARK_MASTER_HOST, to bind the master to a different IP address or hostname
# - SPARK_MASTER_PORT / SPARK_MASTER_WEBUI_PORT, to use non-default ports for the master
# - SPARK_MASTER_OPTS, to set config properties only for the master (e.g. "-Dx=y")
# - SPARK_WORKER_CORES, to set the number of cores to use on this machine
# - SPARK_WORKER_MEMORY, to set how much total memory workers have to give executors (e.g. 1000m, 2g)
# - SPARK_WORKER_PORT / SPARK_WORKER_WEBUI_PORT, to use non-default ports for the worker
# - SPARK_WORKER_DIR, to set the working directory of worker processes
# - SPARK_WORKER_OPTS, to set config properties only for the worker (e.g. "-Dx=y")
# - SPARK_DAEMON_MEMORY, to allocate to the master, worker and history server themselves (default: 1g).
# - SPARK_HISTORY_OPTS, to set config properties only for the history server (e.g. "-Dx=y")
# - SPARK_SHUFFLE_OPTS, to set config properties only for the external shuffle service (e.g. "-Dx=y")
# - SPARK_DAEMON_JAVA_OPTS, to set config properties for all daemons (e.g. "-Dx=y")
# - SPARK_DAEMON_CLASSPATH, to set the classpath for all daemons
# - SPARK_PUBLIC_DNS, to set the public dns name of the master or workers

export SPARK_MASTER_HOST=hadoop
export SPARK_MASTER_PORT=7077
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=1g


# Options for launcher
# - SPARK_LAUNCHER_OPTS, to set config properties and Java options for the launcher (e.g. "-Dx=y")

# Generic options for the daemons used in the standalone deploy mode
# - SPARK_CONF_DIR      Alternate conf dir. (Default: ${SPARK_HOME}/conf)
# - SPARK_LOG_DIR       Where log files are stored.  (Default: ${SPARK_HOME}/logs)
# - SPARK_LOG_MAX_FILES Max log files of Spark daemons can rotate to. Default is 5.
# - SPARK_PID_DIR       Where the pid file is stored. (Default: /tmp)
# - SPARK_IDENT_STRING  A string representing this instance of spark. (Default: $USER)
# - SPARK_NICENESS      The scheduling priority for daemons. (Default: 0)
# - SPARK_NO_DAEMONIZE  Run the proposed command in the foreground. It will not output a PID file.
# Options for native BLAS, like Intel MKL, OpenBLAS, and so on.
# You might get better performance to enable these options if using native BLAS (see SPARK-21305).
# - MKL_NUM_THREADS=1        Disable multi-threading of Intel MKL
# - OPENBLAS_NUM_THREADS=1   Disable multi-threading of OpenBLAS

export JAVA_HOME=/opt/apps/jdk1.8.0_261
export SCALA_HOME=/opt/apps/scala-2.12.8
```