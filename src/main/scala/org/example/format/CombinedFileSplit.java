package org.example.format;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CombinedFileSplit extends FileSplit {

    private List<FileSplit> splits;
    private long length = 0;

    public CombinedFileSplit() {
        splits = new ArrayList<>();
    }

    public void addSplit(FileSplit split) {
        splits.add(split);
        length += split.getLength();
    }

    public List<FileSplit> getSplits() {
        return splits;
    }

    @Override
    public Path getPath() {
        if (splits.isEmpty()) {
            return new Path("");
        }
        return splits.get(0).getPath();
    }

    public long getStart() {
        if (splits.isEmpty()) {
            return 0;
        }
        return splits.get(0).getStart();
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public String[] getLocations() throws IOException {
        List<String> list = splits.stream().flatMap(inputSplit -> {
            try {
                String[] locations = inputSplit.getLocations();
                return Arrays.stream(locations);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).toList();
        String[] locations = new String[list.size()];
        list.toArray(locations);
        return locations;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(splits.size());
        out.writeLong(length);
        for (FileSplit split : splits) {
            split.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        length = in.readLong();
        for (int i = 0; i < size; i++) {
            FileSplit split = new FileSplit();
            split.readFields(in);
            splits.add(split);
        }
    }
}
