package com.example.rocksdb;

import com.example.rocksdb.MetaStorage;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RocksDBStorage implements MetaStorage {

    public static void main(String[] args) throws RocksDBException {
        if (args.length < 1) {
            System.out.println(
                    "usage: RocksDBColumnFamilySample db_path");
            System.exit(-1);
        }

        final String db_path = args[0];
        try(final Options options = new Options().setCreateIfMissing(true);
            final RocksDB db = RocksDB.open(options, db_path)) {

            assert(db != null);

            // create column family
            try(final ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(
                    new ColumnFamilyDescriptor("new_cf".getBytes(),
                            new ColumnFamilyOptions()))) {
                assert (columnFamilyHandle != null);
            }
        }

        // open DB with two column families
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        // have to open default column family
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
        // open the new one, too
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor("new_cf".getBytes(), new ColumnFamilyOptions()));
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        try (final DBOptions options = new DBOptions().setCreateIfMissing(true);
             final RocksDB db = RocksDB.open(options, db_path,
                     columnFamilyDescriptors, columnFamilyHandles)) {
            assert (db != null);

            try {
                // put and get from non-default column family
                db.put(columnFamilyHandles.get(1), new WriteOptions(), "key".getBytes(), "value".getBytes());

                // atomic write
                try (final WriteBatch wb = new WriteBatch()) {
                    wb.put(columnFamilyHandles.get(0), "key2".getBytes(),
                            "value2".getBytes());
                    wb.put(columnFamilyHandles.get(1), "key3".getBytes(),
                            "value3".getBytes());
                  //  wb.delete(columnFamilyHandles.get(1), "key".getBytes());
                    db.write(new WriteOptions(), wb);
                }
                db.flush(new FlushOptions().setWaitForFlush(true));

                Map<String, TableProperties> properties = db.getPropertiesOfAllTables();

                List<Range> ranges = new ArrayList<>();
                ranges.add(new Range(new Slice(new byte[]{0}), new Slice(new byte[]{-0x80})));
                Map<String, TableProperties> properties2 = db.getPropertiesOfTablesInRange(ranges);
                // drop column family
                db.dropColumnFamily(columnFamilyHandles.get(1));
            } finally {
                for (final ColumnFamilyHandle handle : columnFamilyHandles) {
                    handle.close();
                }
            }
        }
    }
}
