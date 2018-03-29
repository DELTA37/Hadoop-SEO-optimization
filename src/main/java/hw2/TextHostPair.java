package hw2;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.*;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class TextHostPair implements WritableComparable<TextHostPair> {
    private Text query;
    private Text host;

    public TextHostPair() {
        set(new Text(), new Text());
    }

		public static String parseHost(String url) {
			URI uri;
			try {
				uri = new URI(url);
			} catch(URISyntaxException e) {
				return null;
			}
			String host = uri.getHost();
			if (host != null && host.startsWith("www.")) {
				host = host.substring(4);
			}
			return host;
		}

    public TextHostPair(String query, String host) {
        set(new Text(query), new Text(host));
    }

    public TextHostPair(Text query, Text host) {
        set(query, host);
    }

    private void set(Text query, Text host) {
        this.query = query;
        this.host = host;
    }

    public void set(TextHostPair other) {
        this.query.set(other.query);
        this.host.set(other.host);
    }

    public Text getQuery() {
        return query;
    }

    public Text getHost() {
        return host;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        query.write(out);
        host.write(out);
    }

    @Override
    public int compareTo(@Nonnull TextHostPair o) {
        int cmp = host.compareTo(o.host);
        return (cmp == 0) ? -query.compareTo(o.query) : cmp;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        query.readFields(dataInput);
        host.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        return query.hashCode() * 163 + host.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TextHostPair) {
            TextHostPair tp = (TextHostPair) obj;
            return host.equals(tp.host) && query.equals(tp.query);
        }
        return false;
    }

    @Override
    public String toString() {
        return host + "\t" + query;
    }
}
