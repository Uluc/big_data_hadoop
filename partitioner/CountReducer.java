package partitioner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<Text, Text, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {


	  //Loop through the values, for each iteration increase counter by 1	 
	  int counter = 0;	  
	  for (Text value : values) {
		  counter++;
	  }
	  /*
       * Call the write method on the Context object to emit a key
       * and a value from the reduce method.
       */
	  context.write(key, new IntWritable(counter));
  }
}
