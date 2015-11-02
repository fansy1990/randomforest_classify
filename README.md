开发环境：
Intellij IDEA14 、Maven3.2、JDK1.7、Hadoop2.6 、mahout0.10

运行：
1. descirbe :
./mahout describe -p naivebayes.txt -f naivebayes.info -d L 13 N

2. build forest :
./mahout buildforest -d naivebayes.txt -ds naivebayes.info -sl 5 -sd 11 -t 5 -p -o rf_model

3. test forest :
./mahout testforest -i naivebayes.txt -o rf_out -ds naivebayes.info -m rf_model -a -mr

4. classify unlabeled data with source data
./yarn jar randomforest_classify1.0.jar algorithm.ClassifyDriver
-i naivebayes_predict.txt -o rf_classify_addSource -splitter , -addSource true -model rf_model -dataset naivebayes.info

5. classify unlabeled data without source data
./yarn jar randomforest_classify1.0.jar algorithm.ClassifyDriver
-i naivebayes_predict.txt -o rf_classify -splitter , -addSource false -model rf_model -dataset naivebayes.info

说明：
其中，1、2、3直接使用mahout的命令行运行即可；4、5使用打包好的jar运行即可

用法：
Usage: algorithm.ClassifyDriver -i input -o output -splitter splitter -addSource addSource -model modelPath -dataset dataset
-i input 输入文件
-o output 输出文件
-splitter splitter 输入数据分隔符
-addSource addSource true/false是否把输入加入到每个输出
-model modelPath 模型文件路径
-dataset dataset dataset路径

注意点：
1. jar包需要修改里面的hadoop.properties为自定义集群配置；
2. 数据naivebayes.txt 、 naivebayes_predict.txt 需上传到HDFS；