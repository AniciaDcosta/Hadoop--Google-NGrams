package googlengram.main.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* This reducer class of the second cycle. It is used as
 * both reducer and combiner for the second cycle
 * It basically concatenates all the four grams containing the one gram data
 * It uses $ sign to differentiate between two  4 grams
 * 
 * example
 * input : dog  #40
 *         dog  dog is faithful animal
 *         is   dog is faithful animal
 *         faithful dog is faithful animal
 *         animal   dog is faithful animal
 * 
 * output : dog#40  dog is faithful animal$ 
 * 
 * Author: Anicia Dcosta
 */

public class NgramReducer extends Reducer<Text, Text, Text, Text> {

	//private Text gram = new Text();
	//private Text gramValue = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// temp is used to save all the four grams which contain a specific one
		// gram
		StringBuilder temp = new StringBuilder();
		int constantString = 33554432;
		// "%" used in the next Mapper for splitting the data
		temp.append("%");
		// Temporary storage string
		String occurrence = new String();
		// iterator through the input to group all the four grams connected to a
		// specific one gram
		Iterator<Text> value = values.iterator();
		while (value.hasNext()) {
			String currentString = value.next().toString();
			// if the value is coming from a one grams
			// just store and add it to the key
			if (currentString.startsWith("#")) {
				occurrence = currentString;
			} else {
				// If the gram is a 4Gram then simply add the 4gram string to
				// the string builder.
				if(((currentString.length()) + (temp.length())) < constantString)
				temp.append(currentString + "$");
			}
		}
		String finalKey = key.toString() + occurrence;
		// The key being returned is given it's frequency value
		// writing the output.The values returned are not altered from their
		// input state
		// just checking the length of the String builder,
		// If it does not contain any other four grams related to it
		// just drop the percentage sign ("%")
		if (temp.length() == 1) {
			temp.deleteCharAt(0);
		}
		//gramValue.set(temp.toString());
		// if the final key does not contain "#" symbol it not a 1gram ,its
		// 1gram that is formed by splitting four grams and it is not relevant
		// data ,therefore this if condition
		if (finalKey.contains("#")) {
			// writing the output
			context.write(new Text(finalKey), new Text(temp.toString()));
		}
	}
}
