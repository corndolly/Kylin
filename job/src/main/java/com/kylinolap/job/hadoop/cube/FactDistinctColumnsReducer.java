/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.job.hadoop.cube;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.kylinolap.cube.common.BytesSplitter;
import com.kylinolap.cube.common.SplittedBytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.ByteArray;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.job.constant.BatchConstants;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.metadata.model.cube.CubeDesc;
import com.kylinolap.metadata.model.cube.TblColRef;

/**
 * @author yangli9
 */
public class FactDistinctColumnsReducer extends Reducer<ShortWritable, Text, NullWritable, Text> {

    private List<TblColRef> columnList = new ArrayList<TblColRef>();
    private String intermediateTableRowDelimiter;
    private byte byteRowDelimiter;
    private BytesSplitter bytesSplitter;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata(conf);
        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeDesc cubeDesc = cube.getDescriptor();

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        columnList = baseCuboid.getColumns();

        intermediateTableRowDelimiter = conf.get(BatchConstants.CFG_CUBE_INTERMEDIATE_TABLE_ROW_DELIMITER, Character.toString(BatchConstants.INTERMEDIATE_TABLE_ROW_DELIMITER));
        byteRowDelimiter = intermediateTableRowDelimiter.getBytes("UTF-8")[0];
        bytesSplitter = new BytesSplitter(4096000, 1024); //4M

    }

    @Override
    public void reduce(ShortWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TblColRef col = columnList.get(key.get());

        HashSet<ByteArray> set = new HashSet<ByteArray>();
        for (Text textValue : values) {
//            bytesSplitter.split(textValue.getBytes(), textValue.getLength(), byteRowDelimiter);
//            SplittedBytes[] splitBuffers = bytesSplitter.getSplitBuffers();
//            for (SplittedBytes buffer : splitBuffers) {
//                ByteArray value = new ByteArray(Bytes.copy(buffer.value, 0, buffer.length));
//                set.add(value);
//            }
            ByteArray value = new ByteArray(Bytes.copy(textValue.getBytes(), 0, textValue.getLength()));
            set.add(value);
        }

        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        String outputPath = conf.get(BatchConstants.OUTPUT_PATH);
        FSDataOutputStream out = fs.create(new Path(outputPath, col.getName()));

        try {
            for (ByteArray value : set) {
                out.write(value.data);
                out.write('\n');
            }
        } finally {
            out.close();
        }

    }

}
