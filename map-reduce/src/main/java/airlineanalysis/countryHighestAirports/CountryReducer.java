package airlineanalysis.countryHighestAirports;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class CountryReducer
		extends Reducer<Text,Text,Text,Text> {
	private static final HashMap<String, Integer> map = new HashMap<String, Integer>();

	public void reduce(Text key, Iterable<Text> values,
					   Context context
	) throws IOException, InterruptedException {
		insertIntoMap(values);
		getMax(context);
	}

	private void insertIntoMap(Iterable<Text> values){
		for (Text val : values) {
			if (map.containsKey(val.toString())){
				map.put(val.toString(), map.get(val.toString()) + 1);
			} else{
				map.put(val.toString(), 1);
			}
		}
	}

	private void getMax(Context context) throws IOException, InterruptedException {
		int max = Integer.MIN_VALUE;
		String maxCountry = null;
		for (String country: map.keySet()){
			if (max < map.get(country)){
				max = map.get(country);
				maxCountry = country;
			}
		}
		context.write(new Text(maxCountry), new Text(String.valueOf(max)));
	}
}