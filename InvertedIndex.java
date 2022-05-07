import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

// INPUT FORMAT
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.DataOutputBuffer;

// MAP 
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//exception
import java.nio.charset.CharacterCodingException;



import java.util.StringTokenizer;
import java.util.HashMap;



public class InvertedIndex {

  public static class XmlInputFormat extends TextInputFormat {

    public static final String START_TAG_KEY = "<page>";
    public static final String END_TAG_KEY = "</page>";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        try {
            return new XmlRecordReader((FileSplit) split, context);
        } catch (IOException e) {
            throw new RuntimeException("TODO : refactor this...");
        }
    }

    /**
     * XMLRecordReader class to read through a given xml document to output xml
     * blocks as records as specified by the start tag and end tag
     */
    public static class XmlRecordReader extends RecordReader<LongWritable, Text> {
        private final byte[] startTag;
        private final byte[] endTag;
        private final long start;
        private final long end;
        private static  FSDataInputStream fsin;
        private static  DataOutputBuffer buffer = new DataOutputBuffer();

        private LongWritable key = new LongWritable();
        private Text value = new Text();


        public XmlRecordReader(FileSplit split, TaskAttemptContext context) throws IOException {
            Configuration conf = context.getConfiguration();
            conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
            conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
            startTag  = START_TAG_KEY.getBytes("utf-8");
            endTag  = END_TAG_KEY.getBytes("utf-8");

            //startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
            //endTag = conf.get(END_TAG_KEY).getBytes("utf-8");

            // open the file and seek to the start of the split
            start = split.getStart();
            end = start + split.getLength();
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(conf);
            fsin = fs.open(split.getPath());
            fsin.seek(start);
        }
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        }
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (fsin.getPos() < end) {
                if (readUntilMatch(startTag, false)) {
                    try {
                        buffer.write(startTag);
                        if (readUntilMatch(endTag, true)) {
                            key.set(fsin.getPos());
                            value.set(buffer.getData(), 0, buffer.getLength());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }
        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }
        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }
        @Override
        public void close() throws IOException {
            fsin.close();
        }
        @Override
        public float getProgress() throws IOException {
            return (fsin.getPos() - start) / (float) (end - start);
        }
        
        private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
            int i = 0;
            while (true) {
                int b = fsin.read();
                // end of file:
                if (b == -1) return false;
                // save to buffer:
                if (withinBlock) buffer.write(b);

                // check if we're matching:
                if (b == match[i]) {
                    i++;
                    if (i >= match.length) return true;
                } else i = 0;
                // see if we've passed the stop point:
                if (!withinBlock && i == 0 && fsin.getPos() >= end) return false;
            }
        }
    }
}

    public static class TokenizerMapper  extends Mapper<Object, Text, Text, Text>{
  
      private Text word = new Text();
  
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
  
        // Partir Id y los textos

        String[] titleAndText = parseTitleAndText(value);
        
        String pageString = titleAndText[0].replace(' ','_');
        if(notValidPage(pageString))
            return;

        // String DocId = value.toString().substring(0, value.toString().indexOf("\t"));
        //String value_raw =  value.toString().substring(value.toString().indexOf("\t") + 1);
        
        StringTokenizer itr = new StringTokenizer(titleAndText[1], " '-");
        
        while (itr.hasMoreTokens()) {
          word.set(itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase());
          if(word.toString() != "" && !word.toString().isEmpty()){
            context.write(word, new Text(pageString));
          }
        }
      }

      private boolean notValidPage(String pageString) {
          return pageString.contains(":");
      }

      private String[] parseTitleAndText(Text value) throws CharacterCodingException {
        String[] titleAndText = new String[2];
        
        int start = value.find("<title>");
        int end = value.find("</title>", start);
        start += 7; //add <title> length.
        
        titleAndText[0] = Text.decode(value.getBytes(), start, end-start);

        start = value.find("<text");
        start = value.find(">", start);
        end = value.find("</text>", start);
        start += 1;
        
        if(start == -1 || end == -1) {
            return new String[]{"",""};
        }
        
        titleAndText[1] = Text.decode(value.getBytes(), start, end-start);
        
        return titleAndText;
    }
}
  
    public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
  
      public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        
        HashMap<String,Integer> map = new HashMap<String,Integer>();
  
        for (Text val : values) {
          
          if (map.containsKey(val.toString())) {
            map.put(val.toString(), map.get(val.toString()) + 1);
          } else {
            map.put(val.toString(), 1);
          }
        }
        StringBuilder docValueList = new StringBuilder();
        for(String docID : map.keySet()){
          docValueList.append(docID + ":" + map.get(docID) + " ");
        }
        context.write(key, new Text(docValueList.toString()));
      }
    }
  
    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "inverted index");

      job.setJarByClass(InvertedIndex.class);
      // job.setInputFormatClass(TextInputFormat.class);
      job.setInputFormatClass(XmlInputFormat.class);
      job.setMapperClass(TokenizerMapper.class);


      // // Input / Mapper
      // FileInputFormat.setInputPaths(job, new Path(in));
      // job.setInputFormatClass(TextInputFormat.class);
      // job.setMapOutputKeyClass(Text.class);
      // job.setMapOutputValueClass(Text.class);
      // job.setMapperClass(PageRankJob2Mapper.class);
        

  
      job.setReducerClass(IntSumReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
