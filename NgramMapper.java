package googlengram.main.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import java.util.List;
import java.util.ArrayList;

/* This Mapper is of the second job  in just breaking
 * four grams as one grams. 
 * In this Mapper we just break the four grams into 1 grams
 * and assign # sign to the value of one gram data occurrence
 * input  sample : dog  40
 * 	           $dog is faithful animal 40
 * output : dog  #40
 * 	    dog  dog is faithful animal
 * 	    is   dog is faithful animal
 * 	    faithful dog is faithful animal
 * 	    animal   dog is faithful animal
 * 	
 * 
 * Author:  Anicia Dcosta 
 */

public class NgramMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text gram = new Text();
	private Text gramValue = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// converting Text to string
		String currentString = value.toString();
		// variable to store the index
		int tempInt = -1;
		// checking for the data is either one gram or four gram
		// the key with "$" is a four gram data
		if (currentString.startsWith("$")) {
			// getting rid of the $ sign
			String temp = currentString.substring(1);
			// splitting the four grams data using the space as delimiter
			String[] grams = temp.split("\\s+");
			// getting rid of the frequency of four grams, because we are only
			// interested in each four gram occurrence against 1 gram data .
			int size = grams.length;
			int occurenceIndex = temp.indexOf(grams[size - 1]);
			// adding each four gram data as one gram data
			// key being the broken down one gram data and value will be the
			// full four gram data with the frequency of occurrence
			for (int i = 0; i < size - 1; i++) {
				// considering only the validate data
				if (grams[i].matches("[a-zA-Z ,_]+")) {
					if (grams[i].indexOf("_") != -1) {
						tempInt = grams[i].indexOf("_");
						grams[i] = grams[i].substring(0, tempInt);
					// writing the output
					gram.set(grams[i].trim());
					gramValue.set(temp.substring(0, occurenceIndex).trim());
					context.write(gram,gramValue);
				   }
				}
			}
		}
		// else the data is a one gram data
		// do no changes just add "#" sign to differentiate the one gram from
		// four gram
		else {
			// splitting the data with white spaces as delimiters
			String[] parts = currentString.split("\\s+");
			// writing the output
			gram.set(parts[0].trim());
                        gramValue.set("#" + (parts[1].trim()));
                        context.write(gram,gramValue);
		}
	}
}
