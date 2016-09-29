package sz.cluster.hadoop.test;
/**
 * Created by Manos on 30/6/16.
 */

import java.io.File;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;




public class DiversityMapReduce extends Configured implements Tool {

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
       


        Configuration conf = getConf();
      
        Job job = Job.getInstance(conf);

        job.setJobName("average word count");
        
        job.setJarByClass(DiversityMapReduce.class);
        job.setMapperClass(PairMapper.class);

        job.setCombinerClass(DivCalc.class);
        job.setReducerClass(AverageReducerWithRecords.class);

        job.setMapOutputKeyClass(LongWritable.class);
       job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
       job.setOutputValueClass(AvgCount.class);

        FileInputFormat.addInputPath(job, new Path("input.txt"));
        Random r=new Random();
        FileOutputFormat.setOutputPath(job, new Path("hadoop"+r.nextInt(10000)));

        int ret = job.waitForCompletion(true) ? 0 : 1;
        return ret;

    }
static void removeDir(File dir)
{
    Stack<File> s=new  Stack<File>();

    s.push(dir);

    while(!s.empty())
    {
        File f=s.pop();
        if(f.isDirectory())
        {
            File[] children=f.listFiles();
            if(children!=null)
            {
                s.addAll(Arrays.asList(children));
            }

            if(children==null||children.length==0)
            {
                f.delete();
            }
        }else
        {
            f.delete();
        }
    }
}


    public static int getFilesCount(File file) {
        File[] files = file.listFiles();
        int count = 0;
        for (File f : files)
            if (f.isDirectory())
                count += getFilesCount(f);
            else
                count++;

        return count;
    }



    public static void main(String[] args) throws Exception, IOException {





       BasicConfigurator.configure();
      

   
        int res = ToolRunner.run(new Configuration(), new DiversityMapReduce(), args);
        System.exit(res);
    }
}