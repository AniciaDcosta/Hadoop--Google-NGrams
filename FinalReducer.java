package googlengram.main.mapreduce;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/* This is the reducer for the third cylce.
 * It is the last reducer
 * This method removes '#' from the key and it finds
 *  the relation count for each 4gram within the value.
 * key : a 1gram and its occurrence count
 * value :all 4grams that relate to key
 * 
 * input:  dog#50 faitful$1#animal$1#faithful$1
 * output : dog 50 faithful 2 animal 1
 * 
 * Author: Kris Samuelson
 * Tested by : Eric Christensen
 *
 */
public class FinalReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// string to store the key
		String tempKey = key.toString();
		// variable used to store temporary value
		String tempValue = new String();
		// Delimiter variable
		String pound = "#";
		// Delimiter variable
		String space = " ";
		// variable to store the related keys and count of each related word
		Map<String, Integer> relations = new HashMap<String, Integer>();
		// variable to store the index for "_"
		int index;
		// temporary variable
		String subKey = new String();
		//value to store the final value
		StringBuilder builder = new StringBuilder();
		// variable
		int subValue = 0;
		// variable to store intermediate sum
		int sum = 0;
		// variable
		int newValue;
		// Replaces '#' with ' '
		tempKey = tempKey.replaceFirst(pound, space);
		//All string into upper case
		tempKey = tempKey.toUpperCase();
		// iterating through the values
		Iterator<Text> value = values.iterator();
		while (value.hasNext()) {
			tempValue = value.next().toString();
			//value also int Upper case
			tempValue = tempValue.toUpperCase();
			// splitting the words using "#"
			String[] related = tempValue.split(pound);
			// The loop adds ngram relations to the HashMap and increments
			// occurrences
			for (int x = 0; x < related.length; x++) {
				index = related[x].indexOf('$');
				//checking the indexing if the word does not contain "$"
				if (index != -1) {
					//forming the subkey
					subKey = related[x].substring(0, index);
					subValue = Integer
							.parseInt(related[x].substring(index + 1));

					/*
					 * Checks if key is already in relations HashMap if true,
					 * then occurrence count is incremented if false, then
					 * key/value is simply added to HashMap
					 */
					if (relations.containsKey(subKey)) {
						sum = relations.get(subKey) + 1;
						relations.put(subKey, sum);
					} else {
						relations.put(subKey, subValue);
					}
				}
			}
		}
		
		// Places all related 4grams into a String 
		//getting rid for the mapping and adding the keys and values 
		//in the string builder
		for (Map.Entry<String, Integer> entry : relations.entrySet()) {
			String ngram = entry.getKey();
			Integer count = entry.getValue();
			builder.append(ngram + " " + count + " ");
		}
		//writting the output
		tempValue = builder.toString();
		context.write(new Text(tempKey), new Text(tempValue));
	}
}
