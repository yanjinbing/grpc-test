package com.example.rocksdb;

import org.apache.commons.codec.binary.Hex;
import org.rocksdb.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RocksdbSplit {

    public static void main(String[] args) throws RocksDBException {
        byte[] buf = new byte[2];
        for (int i = 0; i < 65536; i++) {
            Bits.putShort(buf, 0, i);
        //    System.out.println(Hex.encodeHex(buf));
            int n = Bits.getShort(buf, 0);
            if ( n != i ) System.out.println("Error " + i);
        }

        inputData(args);
        partitionCount = 4;
        splitData(args);
    }

    public static void inputData(String[] args) throws RocksDBException {
        final String dbPath = args[0];
        deleteDir(new File(dbPath));
        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
                .setMinWriteBufferNumberToMerge(2)
                .setMaxWriteBufferNumber(4)
                .setTargetFileSizeBase(64 * 1024)
                .setWriteBufferSize(64 * 1024)
                .setLevel0FileNumCompactionTrigger(2)
                .setMaxBytesForLevelBase(128 * 1024)
                .setMaxBytesForLevelMultiplier(2)
                .setNumLevels(7)) {

            // list of column family descriptors, first entry must always be default column family
            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                    new ColumnFamilyDescriptor("cf1".getBytes(), cfOpts)
            );

            // a list which will hold the handles for the column families once the db is opened
            final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

            try (final DBOptions options = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);
                 final RocksDB db = RocksDB.open(options, dbPath, cfDescriptors, columnFamilyHandles)) {
                try {
                    for (int i = 0; i < 10000; i++) {
                        db.put(columnFamilyHandles.get(0), getInnerKey(String.format("hello%06d", i)), value);
                    }
                    for (int i = 0; i < 10000; i++) {
                        db.put(columnFamilyHandles.get(1), getInnerKey(String.format("good%06d", i)), value);
                    }
                    db.flush(new FlushOptions().setWaitForFlush(true), columnFamilyHandles);
                    db.compactRange();

                    int partitionId = 0;
                    // 迭代遍历
                    for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                        try (ReadOptions readOptions = new ReadOptions()
                                .setIterateLowerBound(new Slice(shortToByte(partitionId)))
                                .setIterateUpperBound(new Slice(shortToByte(partitionId + 1)));
                             RocksIterator iterator = db.newIterator(columnFamilyHandle, readOptions)) {
                            iterator.seekToFirst();
                            int count = 0;
                            while (iterator.isValid()) {
                                count++;
                                iterator.next();
                            }
                            System.out.println(new String(columnFamilyHandle.getName()) + " put count = " + count);
                        }
                    }
                    System.out.println("入库完成");

                } finally {
                    // NOTE frees the column family handles before freeing the db
                    for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                        columnFamilyHandle.close();
                    }
                } // frees the db and the db options
            }
        }
    }

    public static void splitData(String[] args) throws RocksDBException {
        final String dbPath = args[0];

        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
                .setMinWriteBufferNumberToMerge(2)
                .setMaxWriteBufferNumber(4)
                .setTargetFileSizeBase(64 * 1024)
                .setWriteBufferSize(64 * 1024)
                .setLevel0FileNumCompactionTrigger(2)
                .setMaxBytesForLevelBase(128 * 1024)
                .setMaxBytesForLevelMultiplier(2)
                .setNumLevels(7)) {

            // list of column family descriptors, first entry must always be default column family
            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                    new ColumnFamilyDescriptor("cf1".getBytes(), cfOpts)
            );

            // a list which will hold the handles for the column families once the db is opened
            final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

            try (final DBOptions options = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);
                 final RocksDB db = RocksDB.open(options, dbPath, cfDescriptors, columnFamilyHandles)) {
                try {

                    // 迭代遍历
                    for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                        try (ReadOptions readOptions = new ReadOptions();
                             RocksIterator iterator = db.newIterator(columnFamilyHandle, readOptions)) {
                            iterator.seekToFirst();
                            WriteBatch batch = new WriteBatch();

                            while (iterator.isValid()) {
                                // 读取key，重新计算id，如果id发生改变，创建新的id，并且删除旧的。
                                byte[] key = iterator.key();
                                int id = Bits.getShort(key,0);
                                int code = Bits.getShort(key,key.length-2);
                                if ( code % partitionCount != id){
                                    batch.delete(columnFamilyHandle, key);
                                    Bits.putShort(key, 0, code % partitionCount);
                                    batch.put(columnFamilyHandle, key, iterator.value());
                                }
                                iterator.next();
                            }
                            db.write(new WriteOptions(), batch);
                        }
                    }

                    db.flush(new FlushOptions().setWaitForFlush(true), columnFamilyHandles);
                    db.compactRange();


                    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                        // 迭代遍历
                        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                            try (ReadOptions readOptions = new ReadOptions()
                                    .setIterateLowerBound(new Slice(shortToByte(partitionId)))
                                    .setIterateUpperBound(new Slice(shortToByte(partitionId + 1)));
                                 RocksIterator iterator = db.newIterator(columnFamilyHandle, readOptions)) {
                                iterator.seekToFirst();
                                int count = 0;
                                while (iterator.isValid()) {
                                    count++;
                                    iterator.next();
                                }
                                System.out.println(new String(columnFamilyHandle.getName()) + " " + partitionId
                                        + " put count = " + count);
                            }
                        }
                    }
                    System.out.println("更新完成");


                } finally {
                    // NOTE frees the column family handles before freeing the db
                    for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                        columnFamilyHandle.close();
                    }
                } // frees the db and the db options
            }
        }
    }

    static class Bits {
        public static void putShort(byte[] buf, int offSet, int x) {
            buf[offSet] = (byte) (x >> 8);
            buf[offSet + 1] = (byte) (x);
        }

        public static int getShort(byte[] buf, int offSet) {
            int x = buf[offSet] & 0xff;
            x = (x  << 8) + (buf[offSet + 1] & 0xff);
            return x;
        }
    }

    public static int getHashCode(byte[] key) {
        return Arrays.hashCode(key) & 0xFFFF;
    }

    public static byte[] getInnerKey(byte[] key){
        ByteBuffer buffer = ByteBuffer.allocate(key.length + 4);
        int code = getHashCode(key);
        buffer.putShort((short) (code % partitionCount));
        buffer.put(key);
        buffer.putShort((short)code);
        return buffer.array();
    }

    public static InnerKey toInnerKey(byte[] key){
        int l = key.length;
        InnerKey innerKey = new InnerKey();
        ByteBuffer buffer = ByteBuffer.wrap(key);
        innerKey.id = buffer.getShort();
        innerKey.code = buffer.getShort(buffer.limit());
        return innerKey;
    }

    static class InnerKey{
        int id;
        byte[] key;
        int code;
    }



    public static byte[] shortToByte(int id){
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.putShort((short) (id));
        return buffer.array();
    }

    public static byte[] getInnerKey(String key){
        return getInnerKey(key.getBytes(StandardCharsets.UTF_8));
    }

    private static int partitionCount = 2;
    private static byte[] value;

    static {
        value = new byte[128];
        for (int i = 0; i < 128; i++)
            value[i] = (byte) (i % 0x70);
    }

    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                deleteDir(file);
            }
        }
        return dir.delete();
    }
}
