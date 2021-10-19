package lastrequest.copy;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* 
 * To define a reduce function for your MapReduce job, subclass 
 * the Reducer class and override the reduce method.
 * The class definition requires four parameters: 
 *   The data type of the input key (which is the output key type 
 *   from the mapper)
 *   The data type of the input value (which is the output value 
 *   type from the mapper)
 *   The data type of the output key
 *   The data type of the output value
 */   
public class RequestReducer extends Reducer<Text, Text, Text, Text> {

  /*
   * The reduce method runs once for each key received from
   * the shuffle and sort phase of the MapReduce framework.
   * The method receives a key of type Text, a set of values of type
   * Text, and a Context object.
   */
 @Override
 public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
	/*
	 * SimpleDateFormat sets the date format for the calendars as:
	 * day/month/year:hour:minute:second
	 */
	SimpleDateFormat f = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss");
	
	/*
	 * latestAccess is initialized and set to 1/1/1 for comparisons using ".after()".
	 * 
	 * lastDate contains the updated date of last access for an ip as
	 * the for loop iterates. Once the for loop is finished iterating,
	 * lastDate is passed as the value of the reducer.
	 * 
	 * currentDate contains the date from the current value in the for loop.
	 * 
	 * currentCalendar contains currentDate as a Calendar object so that
	 * a comparison can be made between currentCalendar and latestAccess.
	 */
	Calendar latestAccess = Calendar.getInstance();
	latestAccess.set(Calendar.MONTH, 1);
	latestAccess.set(Calendar.YEAR, 1);
	latestAccess.set(Calendar.DAY_OF_WEEK, 1);
	String lastDate = new String();
	Date currentDate = null;
	Calendar currentCalendar = Calendar.getInstance();

	for (Text value : values){
		String currentValue = value.toString();
	
		try {
			currentDate = (Date) f.parse(currentValue);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		currentCalendar.setTime(currentDate);

		if(currentCalendar.after(latestAccess)){
			currentDate = currentCalendar.getTime();
			latestAccess.setTime(currentDate);
			lastDate = currentValue;
		}
	}
	/*
	 * Call the write method on the Context object to emit a key
	 * and a value from the reduce method. 
	 */
	context.write(key, new Text(lastDate));   	
 }
}