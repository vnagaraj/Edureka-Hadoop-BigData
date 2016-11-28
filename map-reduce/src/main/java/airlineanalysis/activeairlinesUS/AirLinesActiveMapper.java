package airlineanalysis.activeairlinesUS;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/*
Getting airline name of active airlines from airlines data set
 */

public class AirLinesActiveMapper
		extends Mapper<Object, Text, NullWritable, Text>{

	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String line = value.toString();
		String[] airlineFields = line.split(",");
		String airlineName = airlineFields[1];
		String country = airlineFields[6];
		String active = airlineFields[7];
		if (active.equals("Y") && country.equals("United States"))
			context.write(NullWritable.get(), new Text(airlineName));
	}
}
