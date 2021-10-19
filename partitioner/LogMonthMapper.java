package partitioner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMonthMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public static List<String> months = Arrays.asList("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec");
  /**
   * Example input line:
   * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
   *
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
	  //Split the line on " "
	  String[] line = value.toString().split(" ");
	  //Grab the ip from the line
	  String ip = line[0];
	  String[] ipCheck = new String[4];
	  int counter = 0;
	  Boolean STANDARD_FORMAT = true;
	  //Split the ip on "."
	  for (String ipSplit : ip.split(".")){
          ipCheck[counter] = ipSplit;
          counter++;
          if (ipSplit.length() == 0){ //if the value of split has 0 characters, line does not conform to expected format
        	  STANDARD_FORMAT = false;
          }
      }
	  //if the ip is in the expected format, get the date then output the key,value pair
	  if (STANDARD_FORMAT){
		  String[] date = line[3].split("/");
		  if (date.length > 1) {
			  String currentMonth = date[1];
		        
		        /*
		         * Call the write method on the Context object to emit a key
		         * and a value from the map method.
		         */
		        context.write(new Text(ip), new Text(currentMonth));
	      }
	  }
  }
}
