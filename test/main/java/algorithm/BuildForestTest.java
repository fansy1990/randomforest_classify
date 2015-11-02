package algorithm;

import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.df.mapreduce.BuildForest;
import org.apache.mahout.classifier.df.mapreduce.TestForest;
import org.apache.mahout.classifier.df.tools.Describe;
import util.HadoopUtils;

/**
 * Created by Fansy on 2015/11/2.
 */
public class BuildForestTest {
    public static void main(String[] args)throws Exception{
        // first describe the data naivebayes.txt
        // naivebayes.txt:
        //1,14.23,1.71,2.43,15.6,127,2.8,3.06,.28,2.29,5.64,1.04,3.92,1065
        // label,x1,x2,...x13

        // first describe
        String[] arg =new String[]{
            "-p", HadoopUtils.getNamenode()+"/user/Administrator/naivebayes.txt",
                "-f",HadoopUtils.getNamenode()+"/user/Administrator/naivebayes.info",
                "-d","L","13","N"
        };
        HadoopUtils.delHDFS(HadoopUtils.getNamenode()+"/user/Administrator/naivebayes.info");
        Describe.main(arg);

        // second build forest
        arg=new String[]{
            "-d",HadoopUtils.getNamenode()+"/user/Administrator/naivebayes.txt",
                "-ds",HadoopUtils.getNamenode()+"/user/Administrator/naivebayes.info",
                "-sl","5",
                "-sd","11",
                "-t","5",
                "-p",
                "-o",HadoopUtils.getNamenode()+"/user/Administrator/rf_model",
        };
        HadoopUtils.delHDFS(HadoopUtils.getNamenode()+"/user/Administrator/rf_model");
//        BuildForest.main(arg);
        ToolRunner.run(HadoopUtils.getConf(),new BuildForest(),arg);

        // test forest

        TestForest.main(arg);

    }

}

