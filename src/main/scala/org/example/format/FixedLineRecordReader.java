package org.example.format;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class FixedLineRecordReader extends RecordReader<LongWritable, Text> {
    public static final String ENCODING = "mapreduce.input.fileinputformat.encoding";
    private long count = 0;
    private int length = 0;
    private Text value;
    private long cursor = 0;
    FSDataInputStream fsDataInputStream;
    private String encoding;
    private final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream(128);

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if (inputSplit instanceof FileSplit fileSplit) {
            long start = fileSplit.getStart();
            fsDataInputStream = fileSplit.getPath().getFileSystem(taskAttemptContext.getConfiguration()).open(fileSplit.getPath());
            fsDataInputStream.seek(start);
            length = (int) fileSplit.getLength();
            encoding = taskAttemptContext.getConfiguration().get(ENCODING, "UTF-8");
        } else {
            throw new UnsupportedOperationException("inputSplit must be FileSplit");
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (length == 0 || cursor >= length) {
            value = null;
            return false;
        }
        boolean curLine = true;
        int index = 0;
        int ch;
        while (curLine && (index + cursor) < length) {
            ch = fsDataInputStream.read();
            switch (ch) {
                case -1, '\n':
                    curLine = false;
                    break;
                case '\r':
                    int c = fsDataInputStream.read();
                    if (c == '\n') {
                        // 兼容windows, 将\r\n视为一个符号,只移动cursor不存储内容
                        cursor++;
                        curLine = false;
                    } else {
                        byteBuffer.write(c);
                    }
                    break;
                default:
                    byteBuffer.write(ch);
            }
            index++;
        }
        cursor += index;
        value = new Text(byteBuffer.toString(encoding));
        count++;
        byteBuffer.reset();
        return true;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return new LongWritable(count);
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (float) cursor / length;
    }

    @Override
    public void close() throws IOException {
        fsDataInputStream.close();
    }
}
