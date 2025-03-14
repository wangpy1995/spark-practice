package org.example.format;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CombinedFixedLineRecordReader extends RecordReader<LongWritable, Text> {
    private List<RecordReader<LongWritable, Text>> recordReaders = new ArrayList<>();
    RecordReader<LongWritable, Text> curReader;
    private int readerIdx = 0;
    private int count = 0;
    private long cursor = 0;
    private long length = 0;
    private Text value;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if (inputSplit instanceof CombinedFileSplit) {
            List<FileSplit> splits = ((CombinedFileSplit) inputSplit).getSplits();
            for (InputSplit split : splits) {
                RecordReader<LongWritable, Text> recordReader = new FixedLineRecordReader();
                recordReader.initialize(split, taskAttemptContext);
                recordReaders.add(recordReader);
                length += split.getLength();
            }
            curReader = recordReaders.get(0);
        } else {
            throw new RuntimeException("inputSplit must be CombinedFileSplit");
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (!curReader.nextKeyValue()) {
            curReader.close();
            readerIdx++;
            if (readerIdx < recordReaders.size()) {
                curReader = recordReaders.get(readerIdx);
            } else {
                return false;
            }
        }
        value = curReader.getCurrentValue();
        count++;
        cursor += value.getLength();
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
        for (int i = readerIdx; i < recordReaders.size(); i++) {
            recordReaders.get(i).close();
        }
    }
}
