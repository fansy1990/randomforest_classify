package algorithm;

import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.df.mapreduce.TestForest;
import util.HadoopUtils;

/**
 * Created by Fansy on 2015/11/2.
 */
public class TestForestTest {
    public static void main(String[] args)throws Exception{

        // first describe
        String[]arg=new String[]{
            "-i",HadoopUtils.getNamenode()+"/user/Administrator/naivebayes.txt",
                "-ds",HadoopUtils.getNamenode()+"/user/Administrator/naivebayes.info",
                "-m",HadoopUtils.getNamenode()+"/user/Administrator/rf_model",
                "-a","-mr",
                "-o",HadoopUtils.getNamenode()+"/user/Administrator/rf_test_model",
        };
        HadoopUtils.delHDFS(HadoopUtils.getNamenode()+"/user/Administrator/rf_test_model");

        // test forest
        ToolRunner.run(HadoopUtils.getConf(),new TestForest(),arg);
//        TestForest.main(arg);

    }

}

