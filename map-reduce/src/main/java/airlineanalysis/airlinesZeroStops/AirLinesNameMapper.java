package airlineanalysis.airlinesZeroStops;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/*
Getting airlineId and name from airlines data set
 */

public class AirLinesNameMapper
		extends Mapper<Object, Text, Text, Text>{

	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String line = value.toString();
		String[] airportFields = line.split(",");
		String airlineId = airportFields[0];
		String airlineName = airportFields[1];
		context.write(new Text(airlineId), new Text("NAME" + airlineName));
	}
}
