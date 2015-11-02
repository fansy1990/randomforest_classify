package algorithm;

import algorithm.classify_mr.ClassifyMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.df.mapreduce.Classifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.HadoopUtils;

import static org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile;

/**
 * Mahout  RandomForest 算法分类不带标签数据
 * Created by Fansy on 2015/11/2.
 */
public class ClassifyDriver extends Configured implements Tool{
    private static final Logger log = LoggerFactory.getLogger(Classifier.class);
    public static void main(String[] args) throws Exception{
        ToolRunner.run(HadoopUtils.getConf(),new ClassifyDriver(),null);
    }

    public int run(String[] args) throws Exception {
        validateArgs(args);
        Configuration conf = getConf();
        conf.set("splitter",splitter);
        conf.setBoolean("addSource", addSource);
        log.info("Adding the dataset to the DistributedCache");
        // put the dataset into the DistributedCache
        addCacheFile(new Path(dataset).toUri(), conf);
        log.info("Adding the decision forest to the DistributedCache");
        addCacheFile(new Path(modelPath).toUri(), conf);
        FileSystem fs = FileSystem.get(conf);
        Path inPath = new Path(input);

        Path outPath = new Path(output);
        if(fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        Job job = Job.getInstance(conf,"Random Forest Classify Job with input :"+input);

        job.setJarByClass(ClassifyDriver.class);
        job.setMapperClass(ClassifyMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 验证参数
     * @param args
     */
    private  void validateArgs(String[] args){
        for(int i=0;i<args.length;){
            if(args[i].startsWith("-")){
                switch (args[i]){
                    case "-i":
                        input = args[++i];
                        i++;
                        break;
                    case "-o":
                        output = args[++i];
                        i++;
                        break;
                    case "-splitter":
                        splitter = args[++i];i++;
                        break;
                    case "-addSource":
                        try{
                            addSource= Boolean.parseBoolean(args[++i]);
                        }catch(ClassCastException e){
                            throw e;
                        }
                        i++; break;
                    case "-model":
                        modelPath=args[++i];i++;
                        break;
                    case "-dataset":
                        dataset=args[++i];i++;
                        break;
                    default:
                        printUsage();
                        throw new RuntimeException("不合法的输入参数："+args[i]);
                }
            }

        }
        if(dataset==null || input==null|| output==null || splitter==null ||modelPath==null ){
            printUsage();
            throw new RuntimeException("不合法的输入参数");
        }
    }

    /**
     * 打印用法
     */
    private static void printUsage(){
        System.out.println("Usage: algorithm.ClassifyDriver -i input -o output -splitter splitter" +
                " -addSource addSource -model modelPath -dataset dataset\n" +
                "-i input 输入文件\n-o output 输出文件 \n" +
                "-splitter splitter 输入数据分隔符\n-addSource addSource true/false是否把输入加入到每个输出\n" +
                "-model modelPath 模型文件路径\n-dataset dataset dataset路径");
    }
    private  String dataset;
    private  String input;
    private  String output ;
    private  String splitter ;
    private  boolean addSource =false ;
    private  String modelPath;
}
