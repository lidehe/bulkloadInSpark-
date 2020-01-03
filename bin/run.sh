#!/bin/bash
#########################################################################
#
# 给脚本的参数:  类名 jar文件 参数
#
# 注意
#     为了不上传太多的无用jar到application的容器里，在 (1) 处稍作过滤，仅给hbase的jar包
#########################################################################

class=${1}

shift
jar=${1}

shift
params=${@}


# （1）hbase*.jar作为过滤条件
hbase_jars=`ls "${HBASE_HOME}"/lib/hbase*.jar`
hbase_jars=`echo ""${hbase_jars}""|sed 's# #,#g'`

spark-submit --master yarn --deploy-mode client --jars ${hbase_jars} --class ${class} ${jar} ${params}
