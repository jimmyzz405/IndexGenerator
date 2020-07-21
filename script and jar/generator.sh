#! /bin/bash
# 与jar包放置于同一目录执行
app_name=generator.jar
echo "输入数字选择想创建的index类型，1=data_instance, 2=instance_relation, 3=attribute_relation"
read value1 
echo "输入ES服务器IP:端口号"
read value2
echo "您选择的index类型：$value1   您的服务器url： $value2" 
echo "==========================Indexing============================"
java -jar $app_name $value1 $value2
exit 0
