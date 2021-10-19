package average;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

	  double avgLength = 0;
	  int wordCount = 0;
	  
	  for (IntWritable value : values) {
		  
		  avgLength += value.get();
		  wordCount++;
	  }
	  avgLength /= wordCount;
	  
	  context.write(key, new DoubleWritable(avgLength));

  }
}