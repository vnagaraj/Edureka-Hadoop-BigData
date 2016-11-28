package airlineanalysis.countryHighestAirports;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/*
list of Airports operating in the Country India
 */

public class CountryMapper
		extends Mapper<Object, Text, Text, Text>{

	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String line = value.toString();
		String[] airportFields = line.split(",");
		String country = airportFields[3];
		context.write(new Text("COMMON KEY"), new Text(country));

	}
}
