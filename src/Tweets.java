import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * Find out users, whose have the most characters in tweets a day.
 * Input data: http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip
 */
public class Tweets {

	/*
	 * Regular expression for parsing data (it's quite complicated, because in
	 * some situations data contains comma inside quotes, so we don't want to
	 * parse it).
	 */
	private static final String SPLIT_REGEX = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

	/*
	 * Date format for parsing input date.
	 */
	private static final SimpleDateFormat DATE_FORMAT_CSV = new SimpleDateFormat(
			"EEE MMM d HH:mm:ss zzz yyyy");

	/*
	 * Date format for parsing readable output date.
	 */
	private static final SimpleDateFormat DATE_FORMAT_OUTPUT = new SimpleDateFormat(
			"dd.MM.yyyy");

	/*
	 * Date format for parsing value for map
	 */
	private static final SimpleDateFormat DATE_FORMAT_CUSTOM = new SimpleDateFormat(
			"yyyyMMdd");

	/*
	 * Custom mapper.
	 */
	public static class TweetsMapper extends
			Mapper<LongWritable, Text, DayNick, VIntWritable> {

		/*
		 * Function for removing quotes from start and also end.
		 */
		private static String removeQuotes(String string) {
			String newString = string;
			while (newString.startsWith("\"")) {
				newString = newString.substring(1);
			}
			while (newString.endsWith("\"")) {
				newString = newString.substring(0, newString.length() - 1);
			}
			return newString;
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, DayNick, VIntWritable>.Context context)
				throws IOException, InterruptedException {

			String[] values = value.toString().split(SPLIT_REGEX);

			Date date;
			try {
				date = DATE_FORMAT_CSV.parse(removeQuotes(values[2]));
			} catch (ParseException e) {
				return; // If input data are incorrect, we skip them.
			}

			String nick = removeQuotes(values[4]);
			int textLength = removeQuotes(values[5]).length();
			int dateInt = Integer.parseInt(DATE_FORMAT_CUSTOM.format(date));

			DayNick k = new DayNick(dateInt, nick);
			// VInt can bere here, because the maximum length of tweet is 140 characters, so Int is wasting of memory (VInt can have 2 bytes here)
			VIntWritable v = new VIntWritable(textLength);

			context.write(k, v);
		}

	}

	/*
	 * Custom combiner.
	 */
	public static class TweetsCombiner extends
			Reducer<DayNick, VIntWritable, DayNick, VIntWritable> {

		@Override
		protected void reduce(
				DayNick key,
				Iterable<VIntWritable> values,
				Reducer<DayNick, VIntWritable, DayNick, VIntWritable>.Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			Iterator<VIntWritable> iterator = values.iterator();
			while (iterator.hasNext()) { // sum up values user tweets in one day
				sum += iterator.next().get();
			}
			// maximum number of tweets a day is 2400, so 2400*140 = max 3 bytes. So we can save 1 byte here.
			context.write(key, new VIntWritable(sum));
		}
	}

	public static class TweetsReducer extends
			Reducer<DayNick, VIntWritable, Text, VIntWritable> {

		@Override
		protected void reduce(DayNick key, Iterable<VIntWritable> values,
				Reducer<DayNick, VIntWritable, Text, VIntWritable>.Context context)
				throws IOException, InterruptedException {

			Date date;
			try {
				date = DATE_FORMAT_CUSTOM.parse(Integer.toString(key.getDate().get()));
			} catch (ParseException e) {
				return;
			}
			
			int max = 0;
			int currentSum = 0;
			String maxNick = null;
			String currentNick = key.getNick().toString();
			
			// readable output date
			String dateString = DATE_FORMAT_OUTPUT.format(date);

			Iterator<VIntWritable> iterator = values.iterator();
			while (iterator.hasNext()) {
				VIntWritable value = iterator.next();
				String nick = key.getNick().toString();
				// if there's still previous user (they are grouped together), just sum value
				if (currentNick.equals(nick)) { 
					currentSum += value.get();
					// if there's another user, we need to check for new maximum
				} else { 
					if (currentSum > max) {
						max = currentSum;
						maxNick = currentNick;
					}
					// actualize current helper params
					currentNick = nick;
					currentSum = value.get();
				}
			}
			// we need to do this in case of the last user for this day has maximum
			if (currentSum > max) {
				max = currentSum;
				maxNick = currentNick;
			}
			context.write(new Text(dateString + " " + maxNick),
					new VIntWritable(max));
		}
	}

	/*
	 * Custom partitioner
	 */
	public static class TweetsPartitioner extends
			Partitioner<DayNick, VIntWritable> {

		@Override
		public int getPartition(DayNick key, VIntWritable value, int partitions) {
			// We put values that have the same day % partitions in one
			// partition, so if data are correctly distributed, partitions have
			// also fairly distributed work. We know, that day is starting on index 6.
			int date = Integer.parseInt(Integer.toString(key.date.get()).substring(6));
			return date % partitions;
		}

	}

	/*
	 * Custom grouper;
	 */
	public static class TweetsGrouper extends WritableComparator {

		protected TweetsGrouper() {
			super(DayNick.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// group same days together
			DayNick aa = (DayNick) a;
			DayNick bb = (DayNick) b;
			return aa.getDate().compareTo(bb.getDate());
		}

	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf(
					"Usage: %s needs two arguments, input and output files\n",
					Tweets.class.getSimpleName());
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Twitter counter");
		job.setJarByClass(Tweets.class);
		job.setMapperClass(TweetsMapper.class);
		job.setPartitionerClass(TweetsPartitioner.class);
		job.setGroupingComparatorClass(TweetsGrouper.class);
		job.setCombinerClass(TweetsCombiner.class);
		job.setReducerClass(TweetsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VIntWritable.class);
		job.setMapOutputKeyClass(DayNick.class);
		job.setMapOutputValueClass(VIntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(3);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
