package hw2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;

import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;

import hw2.TextHostPair;

public class SEO extends Configured implements Tool {

    public static class SeoPartitioner extends Partitioner<TextHostPair, LongWritable> {
        @Override
        public int getPartition(TextHostPair key, LongWritable val, int numPartitions) {
            return Math.abs(key.getHost().hashCode()) % numPartitions;
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(TextHostPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((TextHostPair)a).compareTo((TextHostPair)b);
        }
    }

    public static class SeoGrouper extends WritableComparator {
        protected SeoGrouper() {
            super(TextHostPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text a_host = ((TextHostPair)a).getHost();
            Text b_host = ((TextHostPair)b).getHost();
            return a_host.compareTo(b_host);
        }
    }

  public static class SeoMapper extends Mapper<LongWritable, Text, TextHostPair, LongWritable> {
		@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] splitted = value.toString().split("\t");
			String query = splitted[0];
			String host = TextHostPair.parseHost(splitted[1]);
			if (host == null) {
				context.getCounter("COMMON_COUNTERS", "BadURLS").increment(1);
				return ;
			}
			context.write(new TextHostPair(query, host), one);
		}
  }

  public static class SeoReducer extends Reducer<TextHostPair, LongWritable, Text, LongWritable> {
		@Override
    protected void reduce(TextHostPair word, Iterable<LongWritable> nums, Context context) throws IOException, InterruptedException {
			long min_clicks = context.getConfiguration().getLong(MIN_CLICKS, 1);
			long max_clicks = 0;
			long current_clicks = 0;

			TextHostPair max = new TextHostPair();
			TextHostPair stored = new TextHostPair();
			stored.set(word);

			for (LongWritable clicks : nums) {
				if (stored.getQuery().equals(word.getQuery())) {
					current_clicks += clicks.get();
				} else {
					if (current_clicks > max_clicks) {
						max.set(stored);
						max_clicks = current_clicks;
					}
					stored.set(word);
					current_clicks = clicks.get();
				}
			}
			if (max_clicks >= min_clicks) {
				context.write(new Text(max.toString()), new LongWritable(max_clicks));
			}
		}
  }

  public static class SeoCombiner extends Reducer<TextHostPair, LongWritable, TextHostPair, LongWritable> {
		@Override
    protected void reduce(TextHostPair word, Iterable<LongWritable> nums, Context context) throws IOException, InterruptedException {
			long current_clicks = 0;

			TextHostPair stored = new TextHostPair();
			stored.set(word);

			for (LongWritable clicks : nums) {
				if (stored.getQuery().equals(word.getQuery())) {
					current_clicks += clicks.get();
				} else {
					context.write(stored, new LongWritable(current_clicks));
					stored.set(word);
					current_clicks = clicks.get();
				}
			}
		}
  }


  private Job getJobConf(String inputDir, String outputDir) throws Exception {
		Configuration conf = getConf();
		conf.set(TextOutputFormat.SEPERATOR, "\t");

    Job job = Job.getInstance(getConf(), "seo optimization");

    job.setJarByClass(SEO.class);

		job.setMapperClass(SeoMapper.class);
		job.setCombinerClass(SeoCombiner.class);
		job.setReducerClass(SeoReducer.class);

		job.setPartitionerClass(SeoPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(SeoGrouper.class);

    job.setMapOutputKeyClass(TextHostPair.class);
    job.setMapOutputValueClass(LongWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
		
		Path inputPath = new Path(inputDir);
		FileSystem fs = inputPath.getFileSystem(conf);
		System.out.println("input paths:");
		for (FileStatus status : fs.listStatus(inputPath)) {
			Path folder = status.getPath();
			if (!fs.isDirectory(folder)) {
				continue;
			}
			System.out.println("Folder: " + folder.getName());
			for (FileStatus folderStatus : fs.listStatus(folder)) {
				Path path = folderStatus.getPath();
				if (path.getName().endsWith(".gz")) {
					System.out.println(path.getName());
					TextInputFormat.addInputPath(job, path);
				}
			}
		}
		System.out.println("output path:");
		System.out.println(outputDir);
    TextOutputFormat.setOutputPath(job, new Path(outputDir));
		return job;
  }

	@Override
  public int run(String[] args) throws Exception {
    Job job = getJobConf(args[0], args[1]);
  	return job.waitForCompletion(true) ? 0 : 1;
	}

	static public void main(String[] args) throws Exception {
  	int ret = ToolRunner.run(new SEO(), args);
    System.exit(ret);
	}

	private final static Logger LOG = LoggerFactory.getLogger(SEO.class);
	static final LongWritable one = new LongWritable(1);
	static final String MIN_CLICKS = "mapreduce.reduce.seo.minclicks";
}

