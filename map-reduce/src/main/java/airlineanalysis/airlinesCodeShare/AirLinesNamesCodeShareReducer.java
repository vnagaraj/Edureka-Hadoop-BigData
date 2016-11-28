package airlineanalysis.airlinesCodeShare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirLinesNamesCodeShareReducer
		extends Reducer<Text,Text,Text,Text> {
	private static final Text yCodeshare = new Text("Y");

	public void reduce(Text key, Iterable<Text> values,
					   Context context
	) throws IOException, InterruptedException {
		boolean codeshare = false;
		String name = null;
		for (Text val : values) {
			if (val.toString().startsWith("CODESHARE")){
					codeshare = true;
			}
			else{
				name = val.toString().substring(4);
			}
		}
		if (codeshare) {
			if (name != null)
				context.write(new Text(name), yCodeshare);
		}
	}
}