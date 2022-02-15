package com.example.rocksdb;

import org.apache.commons.codec.binary.Hex;
import org.rocksdb.*;
import org.rocksdb.util.BytewiseComparator;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RocksDBStorage {

    private static byte[] value;

    static {
        value = new byte[128];
        for (int i = 0; i < 128; i++)
            value[i] = (byte) (i % 0x70);
    }

    public static void main(String[] args) throws RocksDBException, InterruptedException {
        if (args.length < 1) {
            System.out.println("usage: RocksDBColumnFamilySample db_path");
            System.exit(-1);
        }
        //testSplit(args);
        //testPut(args);
        //exportSST(args[0], 1);
        testSST();
    }

    public static void testSST() throws RocksDBException {
        String target = "/home/tmp/1.sst";
        try (EnvOptions envOptions = new EnvOptions();
             Options wOptions = new Options();
             SstFileWriter writer = new SstFileWriter(envOptions, wOptions)) {
            writer.open(target);
            long start = System.currentTimeMillis();
            for (int i = 0; i < 1000000; i++) {
                byte[] key = String.format("%8d", i).getBytes();
                writer.put(key, value);
            }
            System.out.println("time = " + (System.currentTimeMillis() - start));
            writer.finish();
            System.out.println("time = " + (System.currentTimeMillis() - start));
        }
    }

    public static void exportSST(String dbPath, int partId) throws RocksDBException {
        byte[] startKey = intToBytesForPartId(partId);
        byte[] endKey = intToBytesForPartId(partId + 1);
        String target = "tmp/1.sst";

        new File(target).getParentFile().mkdirs();

        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<ColumnFamilyDescriptor>();
        List<byte[]> columnFamilyBytes = RocksDB.listColumnFamilies(new Options(), dbPath);
        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
        if (columnFamilyBytes.size() > 0) {
            for (byte[] columnFamilyByte : columnFamilyBytes) {
                cfDescriptors.add(new ColumnFamilyDescriptor(columnFamilyByte, cfOptions));
            }
        }
        int kNumInternalBytes = 8;      //internal key 增加的8个字节后缀
        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()) {

            final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

            try (final DBOptions options = new DBOptions();
                 final RocksDB db = RocksDB.open(options, dbPath, cfDescriptors, columnFamilyHandles)) {
                Snapshot snapshot = db.getSnapshot();
                long startSeqNum = 0;
                ColumnFamilyHandle gvHandle = null;
                for (ColumnFamilyHandle handle : columnFamilyHandles) {
                    if (new String(handle.getName()).equalsIgnoreCase("g+v"))
                        gvHandle = handle;
                }

                RocksIterator iterator = db.newIterator(gvHandle, new ReadOptions()
                        .setSnapshot(snapshot)
                        .setIterStartSeqnum(startSeqNum)
                        .setIterateLowerBound(new Slice(startKey))
                        .setIterateUpperBound(new Slice(endKey)));
                iterator.seekToFirst();

                try (EnvOptions envOptions = new EnvOptions();
                     Options wOptions = new Options();
                     SstFileWriter writer = new SstFileWriter(envOptions, wOptions)) {
                    writer.open(target);
                    while (iterator.isValid()) {
                        byte[] key = iterator.key();
                        if (startSeqNum > 0)
                            key = Arrays.copyOfRange(iterator.key(), 0, iterator.key().length - kNumInternalBytes);
                        writer.put(key, iterator.value());

                        iterator.next();
                    }
                    writer.finish();
                }
            }
        }
    }

    public static byte[] intToBytesForPartId(int v) {
        short s = (short) v;
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN);
        buffer.putShort(s);
        return buffer.array();
    }

    public static void testPut(String[] args) throws RocksDBException, InterruptedException {
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

            try (final DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
                 final RocksDB db = RocksDB.open(options, dbPath, cfDescriptors, columnFamilyHandles)) {

                try {

                    for (int i = 0; i < 1000; i++) {
                        db.put(columnFamilyHandles.get(1), String.format("hello%06d", i).getBytes(), value);
                    }
                    for (int i = 0; i < 1000; i++) {
                        db.put(columnFamilyHandles.get(0), String.format("good%06d", i).getBytes(), value);
                    }
                    long seqNo = db.getLatestSequenceNumber() + 1;


                    for (int i = 0; i < 10; i++) {
                        db.put(columnFamilyHandles.get(1), String.format("second%06d", i).getBytes(), value);
                    }
                    Snapshot snapshot = db.getSnapshot();
                    for (int i = 3; i < 10; i++) {
                        db.put(columnFamilyHandles.get(1), String.format("third%06d", i).getBytes(), value);
                    }
                    for (int i = 5; i < 10; i++) {
                        db.put(columnFamilyHandles.get(1), String.format("third%06d", i).getBytes(), value);
                    }

                    {
                        RocksIterator iterator = db.newIterator(columnFamilyHandles.get(1),
                                new ReadOptions().setSnapshot(snapshot)
                                        .setIterStartSeqnum(seqNo));
                        iterator.seekToFirst();
                        while (iterator.isValid()) {
                            byte[] key = iterator.key();
                            System.out.println(new String(key, 0, key.length - 8) + " "
                                    + Hex.encodeHexString(ByteBuffer.wrap(key, key.length - 8, 8)));
                            iterator.next();
                        }
                    }

                    db.flush(new FlushOptions().setWaitForFlush(true), columnFamilyHandles);
                    db.compactRange();
                    System.out.println("入库完成，等待优化");


                    {
                        Slice upperBound = new Slice(new byte[]{-1});

                        final ReadOptions readOptions = new ReadOptions()
                                .setIterStartSeqnum(seqNo)
                                .setIterateUpperBound(new Slice(new byte[]{-1}));
                        final RocksIterator iterator = db.newIterator(columnFamilyHandles.get(1),
                                readOptions);
                        iterator.seekToFirst();


                        int count = 0;
                        try (EnvOptions envOptions = new EnvOptions();
                             Options wOptions = new Options();
                             SstFileWriter writer = new SstFileWriter(envOptions, wOptions)) {
                            writer.open("./tmp/" + "new.sst");
                            while (iterator.isValid()) {
                                writer.put(iterator.key(), iterator.value());
                                iterator.next();
                                count++;
                                System.gc();
                                Thread.sleep(1000);
                            }
                            writer.finish();
                        }
                        System.out.println("export " + count);
                    }

                    System.out.println("getLatestSequenceNumber " + db.getLatestSequenceNumber());

                    for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                        ColumnFamilyMetaData cfMetaData = db.getColumnFamilyMetaData(columnFamilyHandle);
                        System.out.println("columnFamily name " + new String(columnFamilyHandle.getName()));
                        System.out.println("fileCount: " + cfMetaData.fileCount());
                        System.out.println("size: " + cfMetaData.size());
                        for (LevelMetaData levelMetaData : cfMetaData.levels()) {
                            System.out.println("\tlevel: " + levelMetaData.level());
                            System.out.println("\tsize: " + levelMetaData.size());
                            for (SstFileMetaData sst : levelMetaData.files()) {
                                System.out.println("\t\tfileName: " + sst.fileName());
                                System.out.println("\t\tpath: " + sst.path());
                                System.out.println("\t\tsize: " + sst.size());
                                System.out.println("\t\tsmallestSeqno: " + sst.smallestSeqno());
                                System.out.println("\t\tlargestSeqno: " + sst.largestSeqno());
                                System.out.println("\t\tsmallestKey: " + new String(sst.smallestKey()));
                                System.out.println("\t\tlargestKey: " + new String(sst.largestKey()));
                                System.out.println("\t\tnumReadsSampled: " + sst.numReadsSampled());
                                System.out.println("\t\tbeingCompacted: " + sst.beingCompacted());
                                System.out.println("\t\tnumEntries: " + sst.numEntries());
                                System.out.println("\t\tnumDeletions: " + sst.numDeletions());
                                System.out.println("\t\t----------------------------------------------------------");

                            }
                        }
                    }

                    /*
                    批量入库过程，控制每层的文件数量，限制向高层合并。入库完成后，后台启动compact任务，修改参数，向高层合并。
                     */
                } finally {
                    // NOTE frees the column family handles before freeing the db
                    for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                        columnFamilyHandle.close();
                    }
                } // frees the db and the db options

            }

        } // frees the column family options
        System.out.println("OK");
    }

    public static boolean keyInSstFile(SstFileMetaData sst, byte[] startKey, byte[] endKey) {
        AbstractComparator comparator = new BytewiseComparator(new ComparatorOptions());
        byte[] maxStartKey = comparator.compare(ByteBuffer.wrap(sst.smallestKey()), ByteBuffer.wrap(startKey)) <= 0 ?
                startKey : sst.smallestKey();

        byte[] minEndKey = comparator.compare(ByteBuffer.wrap(sst.largestKey()), ByteBuffer.wrap(endKey)) <= 0 ?
                sst.largestKey() : endKey;
        return comparator.compare(ByteBuffer.wrap(maxStartKey), ByteBuffer.wrap(minEndKey)) <= 0;
    }

    public static boolean keyInSstFile(SstFileMetaData sst, byte[] key) {
        AbstractComparator comparator = new BytewiseComparator(new ComparatorOptions());
        boolean r = comparator.compare(ByteBuffer.wrap(sst.smallestKey()), ByteBuffer.wrap(key)) <= 0 &&
                comparator.compare(ByteBuffer.wrap(sst.largestKey()), ByteBuffer.wrap(key)) > 0;
        return r;
    }


    public static void splitSstFile(final String source, final String target, byte[] startKey, byte[] endKey) throws RocksDBException {
        new File(target).getParentFile().mkdirs();
        try (Options options = new Options();
             ReadOptions readOptions = new ReadOptions()
                     .setIterateLowerBound(new Slice(startKey))
                     .setIterateUpperBound(new Slice(endKey));
             SstFileReader reader = new SstFileReader(options)
        ) {
            reader.open(source);
            SstFileReaderIterator iterator = reader.newIterator(readOptions);

            iterator.seekToFirst();
            try (EnvOptions envOptions = new EnvOptions();
                 Options wOptions = new Options();
                 SstFileWriter writer = new SstFileWriter(envOptions, wOptions)) {
                writer.open(target);
                while (iterator.isValid()) {
                    writer.put(iterator.key(), iterator.value());
                    iterator.next();
                }
                writer.finish();
            }
        }
    }


    public static void testSplit(String[] args) throws RocksDBException {

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

            try (final DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
                 final RocksDB db = RocksDB.open(options, dbPath, cfDescriptors, columnFamilyHandles)) {


                byte[] startKey = String.format("hello%06d", 10).getBytes();
                byte[] endKey = String.format("hello%06d", 500).getBytes();


                System.out.println("getLatestSequenceNumber " + db.getLatestSequenceNumber());

                for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                    ColumnFamilyMetaData cfMetaData = db.getColumnFamilyMetaData(columnFamilyHandle);
                    System.out.println("fileCount: " + cfMetaData.fileCount());
                    System.out.println("size: " + cfMetaData.size());
                    for (LevelMetaData levelMetaData : cfMetaData.levels()) {
                        System.out.println("\tlevel: " + levelMetaData.level());
                        System.out.println("\tsize: " + levelMetaData.size());
                        for (SstFileMetaData sst : levelMetaData.files()) {
                            if (!keyInSstFile(sst, startKey, endKey)) break;

                            System.out.println("\t\tfileName: " + sst.fileName());
                            System.out.println("\t\tpath: " + sst.path());
                            System.out.println("\t\tsize: " + sst.size());
                            System.out.println("\t\tsmallestSeqno: " + sst.smallestSeqno());
                            System.out.println("\t\tlargestSeqno: " + sst.largestSeqno());
                            System.out.println("\t\tsmallestKey: " + new String(sst.smallestKey()));
                            System.out.println("\t\tlargestKey: " + new String(sst.largestKey()));
                            System.out.println("\t\tnumReadsSampled: " + sst.numReadsSampled());
                            System.out.println("\t\tbeingCompacted: " + sst.beingCompacted());
                            System.out.println("\t\tnumEntries: " + sst.numEntries());
                            System.out.println("\t\tnumDeletions: " + sst.numDeletions());
                            System.out.println("\t\t----------------------------------------------------------");
                            if (keyInSstFile(sst, startKey) || keyInSstFile(sst, endKey))
                                splitSstFile(sst.path() + sst.fileName(), sst.path() + "new/" + sst.fileName(),
                                        startKey, endKey);
                        }
                    }
                }

                    /*
                    批量入库过程，控制每层的文件数量，限制向高层合并。入库完成后，后台启动compact任务，修改参数，向高层合并。
                     */
            } finally {
                // NOTE frees the column family handles before freeing the db
                for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                    columnFamilyHandle.close();
                }
            } // frees the db and the db options

        }
        // frees the column family options
        System.out.println("OK");
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
