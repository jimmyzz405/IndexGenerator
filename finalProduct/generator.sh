#! /bin/bash
# 与jar包放置于同一目录执行
app_name=generator.jar
server_url=localhost:9200
type=$1
#echo "输入数字选择想创建的index类型，1=data_instance, 2=instance_relation, 3=attribute_relation"
#read value1 
if [ ! $type ]
then 
    echo "请通过脚本参数指定index类型！1 = data_instance, 2 = instance_relation, 3 = attribute_relation"
    exit $?
else
    echo "==========================Indexing============================"
    java -jar $app_name $type $server_url
    exit $?
fi
