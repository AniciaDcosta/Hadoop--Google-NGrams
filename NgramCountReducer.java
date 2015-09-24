package googlengram.main.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.ArrayWritable;

/*This reducer class of the first cycle. It is used as
 * both reducer and combiner for the first cycle
 * It basically finds out the total occurrence for each 1gram/4 gram
 * it sums the number of occurrences for each 1gram/4gram and gives it has output 
 * 
 * Input : 1gram  occurrenceNumber
 *         4grams occurrenceNumber
 * Output : 1gram total_occurrenceNumber
 * 	    $4gram total_occurrenceNumber
 * example
 * input : dog 10
 * 	   dog is faithful animal 40
 * 	   dog 30
 * output : dog  40
 * 	    $dog is faithful animal 40
 * Author: Anicia D'costa
 */

	public class NgramCountReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		// private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {

			// variable to hold the temporary sum
			long sum = 0;
			// iterator through the input to calculate the total views
			Iterator<LongWritable> value = values.iterator();
			while (value.hasNext()) {
				// add the frequency to the total sum
				sum = sum + Long.parseLong(value.next().toString());
				
			}
			//writing the output
			context.write(new Text(key), new LongWritable(sum));
		}

	}
