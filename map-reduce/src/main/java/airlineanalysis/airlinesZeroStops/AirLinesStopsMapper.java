package airlineanalysis.airlinesZeroStops;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/*
Getting airlineId and stops from routes data set
 */

public class AirLinesStopsMapper
		extends Mapper<Object, Text, Text, Text>{

	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String line = value.toString();
		String[] airportFields = line.split(",");
		String airlineId = airportFields[1];
		String stops = airportFields[7];
		context.write(new Text(airlineId), new Text("STOPS" + stops));
	}
}
