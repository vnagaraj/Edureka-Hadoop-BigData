package airlineanalysis.airportsIndia;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/*
list of Airports operating in the Country India
 */

public class AirportsCountryMapper
		extends Mapper<Object, Text, Text, Text>{

	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String line = value.toString();
		String[] airportFields = line.split(",");
		String airPortName = airportFields[1];
		String country = airportFields[3];
		if (country.equals("India")) {
			context.write(new Text(airPortName), new Text(country));
		}
	}
}
