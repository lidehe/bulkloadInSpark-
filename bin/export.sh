#!/bin/bash
#######################################################################################
# 工程简介：
#      用于将数据导出到文件，上传hdfs、hbase
#      支持的数据库为 DB2（本地） 和 MySQL（本地和远程）
# 须知：
#     本项目的运行，需要基于HDFS、HBase、Spark、数据库客户端（db2|MySQL）的指令，所以上述环境是必须的
#     SELECT 字段列表里，一定要把row_key放在第一个。字段列表要与java程序配置文件里qualifiers一致
#
# 遗留问题：
#     1、对于DB2，不知道怎么操作来连接远程数据库
#                                                            李德和   2019年10月31日
#######################################################################################


################################# 变量处理开始 ##############################
# 本程序执行路径
base=$(cd "$(dirname "$0")"; pwd)

# 校验参数个数
if [ $# -ne 3 ]; then
    echo "[ --com.zxftech.rrms.shell: Error--  ]:"
    echo " Three parameters were expected: "
    echo "       1.database type(mysql|db2)"
    echo "       2.table name"
    echo "       3.data date(like:20191030) "
    exit 99
fi

# 数据库类型
db_type=${1}

# 要导出的表
table_name=${2}

## 加载配置信息
source ${base}/export_${table_name}-env.sh

# 要导出的数据截止日期
data_date=${3}

# 要导出的数据截起始日期,如果没有配置，默认是截止日期往前推一个月
date_end=`date -d "${data_date}" +"%Y-%m-%d"`
if test -z ${day};then
    date_begin=`date -d "-${month} month ${date_end}" +"%Y-%m-%d"`
elif test -z "${month}";then
    date_begin=`date -d "-${day} day ${date_end}" +"%Y-%m-%d"`
else
    date_begin=`date -d "-1 month ${date_end}" +"%Y-%m-%d"`
fi

## Java工程编译后的文件，jar放在脚本目录下的lib文件夹内
class_name=com.zxftech.rrms.MyMain
jar_file=${base}/lib/bkload.jar

## 日志文件，DB2执行时会把日志打印到此，如有异常可看此文件
log_file=${base}/${table_name}.log

## SQL file
sql_file=${base}/${table_name}.txt

## 处理sql,首先把sql文件里的日期字符串替换掉，最好把sql也放在env.sh里，这样就少配置了文件了
sql=`echo "${statement}"|sed 's#date_end#'${date_end}'# g'`
sql=`echo "${sql}"|sed 's#date_begin#'${date_begin}'# g'`

## 指定hdfs集群，例：hdfs://host:9000
hdfs_cluster=xxx
input_file=${hdfs_cluster}/hinpu/${table_name}/${data_date}/${table_name}.txt

################################# 变量处理结束 ##############################


################################# 数据处理逻辑开始 ##############################
# 为了支持重跑，先删除hdfs上的该数据
hdfs dfs -rm -r ${input_file}

# 定义一个函数，调用java程序，完成文件转换到HFile(HBase file)
function doTrans(){
    # 先判断待转换-导入的文件是否已经在hdfs上了
    hdfs dfs -test -f ${input_file}
    if [ $? -ne 0 ];then
        echo "[ --com.zxftech.rrms.shell: Error--  ] Input file ${input_file} not exist"
        exit 99
    fi
    # 下面这句一定要执行，或者配置到用户变量里，不然会报错找不到hbse的相关类
    hbase_jars=`ls "${HBASE_HOME}"/lib/hbase*.jar`
    hbase_jars=`echo ""${hbase_jars}""|sed 's# #,#g'`
    spark-submit --master yarn --deploy-mode client --jars ${hbase_jars} --class ${class_name} ${jar_file} ${table_name} ${data_date} &>>${log_file}
    if [ $? -ne 0 ];then
        echo "[ --com.zxftech.rrms.shell: Error--  ] HBase file generate failure,more detail see ${log_file}"
        exit 99
    else
      exit 0
    fi
}

################ 执行导出数据 #############################
if [ ${db_type} == "db2" -a "$(command -v db2)" ];then
    # 连接数据库  格式为： db2 connect to 数据库名 user 登陆名 using 登陆密码
    db2 connect to ${database} user ${user} using ${password} &>${log_file}
    if [ $? -ne 0 ];then
        echo "[ --com.zxftech.rrms.shell: Error--  ] connect to db2 database \"${database}\" error ! More detail see ${log_file}"
        exit 99
    fi
    # 导出数据表到文件
    db2 "export to ${sql_file} of ixf messages ${log_file} ${sql}"
    if [ $? -ne 0 ];then
        echo "[ --com.zxftech.rrms.shell: Error--  ] export failure,more detail see ${log_file}"
        exit 99
    fi
    sed -i 's#\t#'${delimiter}'#g' ${sql_file}
elif [ ${db_type} == "mysql" -a "$(command -v mysql)" ]; then
    mysql -h ${host} -P ${port} -D ${database} -u${user} -p${password} -e "${sql}" > ${sql_file}
    if [ $? -ne 0 ];then
        echo "[ --com.zxftech.rrms.shell: Error--  ] export failure,more detail see ${log_file}"
        exit 99
    fi
    sed -i 's#\t#'${delimiter}'#g' ${sql_file}
else
    echo "[ --com.zxftech.rrms.shell: Warn-- ] Database client not found here! File is considered to had been put on hdfs.Going to load file"
    # 调用map程序转换数据并导入hbase
    doTrans
fi

# 数据上传到 hdfs, 这里要注意，上传的路径要与java程序里读取数据的路径(input)一致
# 先建文件夹，然后上传文件
hdfs dfs -mkdir -p ${hdfs_cluster}/hinpu/${table_name}/${data_date}/
if [ $? -ne 0 ];then
    echo "[ --com.zxftech.rrms.shell: Error--  ] Make dir on hdfs failure,more detail see ${log_file}"
    exit 99
fi
hdfs dfs -put ${base}/${table_name}.txt ${hdfs_cluster}/hinpu/${table_name}/${data_date}/${table_name}.txt &>>${log_file}
if [ $? -ne 0 ];then
    echo "[ --com.zxftech.rrms.shell: Error--  ] Upload to hdfs failure,more detail see ${log_file}"
    exit 99
fi

# 调用map程序转换数据并导入hbase
doTrans

########################################################################################################################
#
#       这部分不需要了，已经放到java代码里了
#
# 执行HBase命令，把转换后的数据导入HBase表中
# 这里要注意，文件路径要与java程序里存放结果文件的路径(output)一致
# hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /hdata/${table_name}/20191030/ ${table_name} &>>${log_file}
# if [ $? -ne 0 ];then
#    echo "import data into HBase failure,more detail see ${log_file}"
#    exit 99
# fi
#########################################################################################################################
