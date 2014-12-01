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
import java.util.*;

import com.kylinolap.common.util.ByteArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.common.BytesSplitter;
import com.kylinolap.cube.common.SplittedBytes;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.dict.DictionaryManager;
import com.kylinolap.job.constant.BatchConstants;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.job.hadoop.hive.JoinedFlatTableDesc;
import com.kylinolap.metadata.model.cube.CubeDesc;
import com.kylinolap.metadata.model.cube.RowKeyDesc;
import com.kylinolap.metadata.model.cube.TblColRef;

/**
 * @author yangli9
 */
public class FactDistinctColumnsMapper<KEYIN> extends Mapper<KEYIN, Text, ShortWritable, Text> {

    private String cubeName;
    private CubeInstance cube;
    private CubeDesc cubeDesc;
    private int[] factDictCols;

    private JoinedFlatTableDesc intermediateTableDesc;
    private String intermediateTableRowDelimiter;
    private byte byteRowDelimiter;
    private BytesSplitter bytesSplitter;

    private ShortWritable outputKey = new ShortWritable();
    private Text outputValue = new Text();
    private Map<ShortWritable, Set<ByteArray>> keyMap = new HashMap<ShortWritable, Set<ByteArray>>();
    private byte[] splitter = new byte[1];

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        intermediateTableRowDelimiter = conf.get(BatchConstants.CFG_CUBE_INTERMEDIATE_TABLE_ROW_DELIMITER, Character.toString(BatchConstants.INTERMEDIATE_TABLE_ROW_DELIMITER));
        byteRowDelimiter = intermediateTableRowDelimiter.getBytes("UTF-8")[0];
        bytesSplitter = new BytesSplitter(200, 4096);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata(conf);
        cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        intermediateTableDesc = new JoinedFlatTableDesc(cubeDesc, null);

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        List<TblColRef> columns = baseCuboid.getColumns();

        ArrayList<Integer> factDictCols = new ArrayList<Integer>();
        RowKeyDesc rowkey = cubeDesc.getRowkey();
        DictionaryManager dictMgr = DictionaryManager.getInstance(config);
        for (int i = 0; i < columns.size(); i++) {
            TblColRef col = columns.get(i);
            if (rowkey.isUseDictionary(col) == false)
                continue;

            String scanTable = (String) dictMgr.decideSourceData(cubeDesc, col, null)[0];
            if (cubeDesc.isFactTable(scanTable)) {
                System.out.println(col + " -- " + i);
                factDictCols.add(i);
            }
        }
        this.factDictCols = new int[factDictCols.size()];
        for (int i = 0; i < factDictCols.size(); i++)
            this.factDictCols[i] = factDictCols.get(i);
        splitter[0] = byteRowDelimiter;
    }

    @Override
    public void map(KEYIN key, Text value, Context context) throws IOException, InterruptedException {

        bytesSplitter.split(value.getBytes(), value.getLength(), byteRowDelimiter);
        SplittedBytes[] splitBuffers = bytesSplitter.getSplitBuffers();

        int[] flatTableIndexes = intermediateTableDesc.getRowKeyColumnIndexes();
        for (int i : factDictCols) {
            outputKey.set((short) i);
            SplittedBytes bytes = splitBuffers[flatTableIndexes[i]];
            outputValue.set(bytes.value, 0, bytes.length);
            System.out.println(i + " -- " + outputValue);
            context.write(outputKey, outputValue);
//            if (keyMap.containsKey(outputKey)) {
//                Set<ByteArray> values = keyMap.get(outputKey);
//                values.add(new ByteArray(Bytes.copy(outputValue.getBytes(), 0, outputValue.getLength())));
//            } else {
//                Set<ByteArray> values = new HashSet<ByteArray>();
//                values.add(new ByteArray(Bytes.copy(outputValue.getBytes(), 0, outputValue.getLength())));
//                keyMap.put(new ShortWritable(outputKey.get()), values);
//            }
        }
    }

    @Override
    protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
//        for (Map.Entry<ShortWritable, Set<ByteArray>> entry : keyMap.entrySet()) {
//            Text t = new Text();
//            for (ByteArray v : entry.getValue()) {
//                t.append(v.data, 0, v.data.length);
//                t.append(splitter, 0, 1);
//            }
//            context.write(entry.getKey(), t);
//        }
    }
}
