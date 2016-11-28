package airlineanalysis.airlinesZeroStops;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirLinesNamesStopsReducer
		extends Reducer<Text,Text,Text,Text> {
	private static final Text zero = new Text("0");

	public void reduce(Text key, Iterable<Text> values,
					   Context context
	) throws IOException, InterruptedException {
		boolean isZero = false;
		String name = null;
		for (Text val : values) {
			if (val.toString().startsWith("STOPS")){
				int stops = Integer.parseInt(val.toString().substring(5));
				if (stops == 0){
					isZero = true;
				}
			}
			else{
				name = val.toString().substring(4);
			}
		}
		if (isZero) {
			if (name != null)
				context.write(new Text(name), zero);
		}
	}
}