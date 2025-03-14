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
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 多个文本文件, 标记\n并行读取
 * 小文件划分进同一个split, 大文件拆分为多个split
 */
public class CombinedFixedLineFileFormat extends FileInputFormat<LongWritable, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(CombinedFixedLineFileFormat.class);
    private long combinedSplitSize = 0;

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit combinedFileSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new CombinedFixedLineRecordReader();
    }

    private CombinedFileSplit addSplitToCombined(CombinedFileSplit combinedSplit, List<InputSplit> splits, FileSplit fileSplit, long maxSize) {
        // 不大于预期大小30% ,也直接加入到combinedSplit中
        if (combinedSplit.getLength() <= 0.3 * maxSize || combinedSplit.getLength() + fileSplit.getLength() <= maxSize) {
            combinedSplit.addSplit(fileSplit);
            LOG.info("Adding to combined: {} {} {}", fileSplit.getStart(), fileSplit.getLength(), fileSplit.getStart() + fileSplit.getLength() + 1);
        } else {
            LOG.info("Combined split: {} {}", combinedSplit.getStart(), combinedSplit.getLength());
            splits.add(combinedSplit);
            combinedSplit = new CombinedFileSplit();
            combinedSplit.addSplit(fileSplit);
        }
        return combinedSplit;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        StopWatch sw = (new StopWatch()).start();
        long minSize = Math.max(this.getFormatMinSplitSize(), getMinSplitSize(job));
        long maxSize = getMaxSplitSize(job);
        List<InputSplit> splits = new ArrayList<>();
        List<FileStatus> files = this.listStatus(job);
        boolean ignoreDirs = !getInputDirRecursive(job) && job.getConfiguration().getBoolean("mapreduce.input.fileinputformat.input.dir.nonrecursive.ignore.subdirs", false);

        CombinedFileSplit combinedSplit = new CombinedFileSplit();
        long totalLength = 0L;
        for (FileStatus file : files) {
            FileSplit fileSplit;
            if (!ignoreDirs || !file.isDirectory()) {
                Path path = file.getPath();
                long length = file.getLen();
                totalLength += length;
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
                                fileSplit = this.makeSplit(path, lastPos, pos - lastPos, blkLocations[blkIndex].getHosts(), blkLocations[blkIndex].getCachedHosts());
                                combinedSplit = addSplitToCombined(combinedSplit, splits, fileSplit, maxSize);
                                lastPos = pos + 1;
                            } else {
                                break;
                            }
                        }
                        // 收尾
                        if (lastPos < length) {
                            int blkIndex = this.getBlockIndex(blkLocations, lastPos);
                            fileSplit = this.makeSplit(path, lastPos, length - lastPos, blkLocations[blkIndex].getHosts(), blkLocations[blkIndex].getCachedHosts());
                            combinedSplit = addSplitToCombined(combinedSplit, splits, fileSplit, maxSize);
                            LOG.info("{}: last split.", fileSplit.getPath().getName());
                        }
                        raf.close();
                    } else {
                        // 文件无法切分
                        if (LOG.isDebugEnabled() && length > Math.min(file.getBlockSize(), minSize)) {
                            LOG.debug("File is not splittable so no parallelization is possible: {}", file.getPath());
                        }
                        fileSplit = this.makeSplit(path, 0L, length, blkLocations[0].getHosts());
                        combinedSplit = addSplitToCombined(combinedSplit, splits, fileSplit, maxSize);
                    }
                } else {
                    // 长度为0
                    fileSplit = this.makeSplit(path, 0L, length, new String[0]);
                    combinedSplit = addSplitToCombined(combinedSplit, splits, fileSplit, maxSize);
                }
            }
        }
        LOG.info("Combined split: {} {}", combinedSplit.getStart(), combinedSplit.getLength());
        splits.add(combinedSplit);

        job.getConfiguration().setLong("mapreduce.input.fileinputformat.numinputfiles", (long) files.size());
        sw.stop();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Total # of splits generated by getSplits: {}, TimeTaken: {}", splits.size(), sw.now(TimeUnit.MILLISECONDS));
        }

        return splits;
    }
}
