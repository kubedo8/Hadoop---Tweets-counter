import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DayNick implements WritableComparable<DayNick> {

	// using Int as date for saving memory
	public IntWritable date;
	public Text nick;

	public DayNick() {
		this(new IntWritable(), new Text());
	}

	public DayNick(int date, String nick) {
		this(new IntWritable(date), new Text(nick));
	}

	public DayNick(IntWritable date, Text nick) {
		this.date = date;
		this.nick = nick;
	}

	public IntWritable getDate() {
		return date;
	}

	public Text getNick() {
		return nick;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		date.readFields(in);
		nick.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		date.write(out);
		nick.write(out);
	}

	@Override
	public int compareTo(DayNick o) {
		int c = date.compareTo(o.getDate());
		return c == 0 ? nick.compareTo(o.getNick()) : c;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof DayNick) {
			DayNick o = (DayNick) obj;
			return date.equals(o.getDate())
					&& nick.equals(nick.equals(o.getNick()));
		}
		return false;
	}

}
