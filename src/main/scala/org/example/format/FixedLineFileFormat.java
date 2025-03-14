package org.example.format;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 单个大文本文件
 * 先等分,再找\n分隔符
 */
public class FixedLineFileFormat extends FileInputFormat<LongWritable, Text> {
    public static final String ENCODING = "mapreduce.input.fileinputformat.encoding";

    private static final Logger LOG = LoggerFactory.getLogger(FixedLineFileFormat.class);

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if (inputSplit instanceof FileSplit) {
            RecordReader<LongWritable, Text> recordReader = new RecordReader<LongWritable, Text>() {
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
            };
            recordReader.initialize(inputSplit, taskAttemptContext);
            return recordReader;
        } else {
            throw new UnsupportedOperationException("inputSplit is not FileSplit");
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        StopWatch sw = (new StopWatch()).start();
        long minSize = Math.max(this.getFormatMinSplitSize(), getMinSplitSize(job));
        long maxSize = getMaxSplitSize(job);
        List<InputSplit> splits = new ArrayList<>();
        List<FileStatus> files = this.listStatus(job);
        boolean ignoreDirs = !getInputDirRecursive(job) && job.getConfiguration().getBoolean("mapreduce.input.fileinputformat.input.dir.nonrecursive.ignore.subdirs", false);

        for (FileStatus file : files) {
            if (!ignoreDirs || !file.isDirectory()) {
                Path path = file.getPath();
                long length = file.getLen();
                if (length != 0L) {
                    BlockLocation[] blkLocations;
                    if (file instanceof LocatedFileStatus) {
                        blkLocations = ((LocatedFileStatus) file).getBlockLocations();
                    } else {
                        FileSystem fs = path.getFileSystem(job.getConfiguration());
                        blkLocations = fs.getFileBlockLocations(file, 0L, length);
                    }

                    if (this.isSplitable(job, path)) {
                        FileSystem fs = path.getFileSystem(job.getConfiguration());
                        FSDataInputStream raf = fs.open(path);
                        long blockSize = file.getBlockSize();
                        long splitSize = this.computeSplitSize(blockSize, minSize, maxSize);
                        int numSplits = (int) (length / splitSize);
                        long pos = 0;
                        long lastPos = pos;
                        for (int i = 1; i < numSplits; i++) {
                            pos += splitSize;
                            if (pos < length) {
                                raf.seek(pos);
                                // 找到下一个行边界
                                while (raf.read() != '\n' && pos < length) {
                                    pos++;
                                    raf.seek(pos);
                                }

                                int blkIndex = this.getBlockIndex(blkLocations, lastPos);
                                splits.add(this.makeSplit(path, lastPos, pos - lastPos, blkLocations[blkIndex].getHosts(), blkLocations[blkIndex].getCachedHosts()));
                                LOG.info("Adding split: {} {} {}", lastPos, pos - lastPos, pos + 1);
                                lastPos = pos + 1;
                            } else {
                                break;
                            }
                        }

                        if (lastPos < length) {
                            int blkIndex = this.getBlockIndex(blkLocations, lastPos);
                            splits.add(this.makeSplit(path, lastPos, length - lastPos, blkLocations[blkIndex].getHosts(), blkLocations[blkIndex].getCachedHosts()));
                            LOG.info("Adding last split: {} {} {}", lastPos, length - lastPos, length);
                        }
                        raf.close();
                    } else {
                        if (LOG.isDebugEnabled() && length > Math.min(file.getBlockSize(), minSize)) {
                            LOG.debug("File is not splittable so no parallelization is possible: {}", file.getPath());
                        }

                        splits.add(this.makeSplit(path, 0L, length, blkLocations[0].getHosts(), blkLocations[0].getCachedHosts()));
                    }
                } else {
                    splits.add(this.makeSplit(path, 0L, length, new String[0]));
                }
            }
        }

        job.getConfiguration().setLong("mapreduce.input.fileinputformat.numinputfiles", (long) files.size());
        sw.stop();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Total # of splits generated by getSplits: {}, TimeTaken: {}", splits.size(), sw.now(TimeUnit.MILLISECONDS));
        }

        return splits;
    }
}
