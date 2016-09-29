package sz.cluster.hadoop.test;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducerWithRecords extends
        Reducer<LongWritable, Text, LongWritable, AvgCount> {

    private LongWritable totalWordCount = new LongWritable();

    @Override
    public void reduce(final LongWritable key, final Iterable<Text> values,
                       final Context context) throws IOException, InterruptedException {

      
        Iterator<Text> iterator = values.iterator();
        AvgCount ret= new AvgCount(0, 0);
        		int cnt=0;
        		double sum=0.0;
        		while (iterator.hasNext()) {
           Text curel = iterator.next();
           
           ret.total_+=Double.parseDouble(curel.toString());
           ret.num_++;
           
        }

        context.write(key, ret);
    }
}