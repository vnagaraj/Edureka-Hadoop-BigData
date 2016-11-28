package airlineanalysis.airlinesCodeShare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/*
Getting airlineId and codeshare from routes data set
 */

public class AirLinesCodeShareMapper
		extends Mapper<Object, Text, Text, Text>{

	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String line = value.toString();
		String[] airportFields = line.split(",");
		String airlineId = airportFields[1];
		String codeShare = airportFields[6];
		if (codeShare.length() != 0)
			context.write(new Text(airlineId), new Text("CODESHARE" + codeShare));
	}
}
