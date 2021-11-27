package org.example;

import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.SnapshotCopierOptions;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotCopier;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotStorage;
import com.google.protobuf.Message;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SnapshotStorageImpl extends LocalSnapshotStorage {
    RaftOutter.SnapshotMeta snapshotMeta = null;
    public SnapshotStorageImpl(final String uri, final RaftOptions raftOptions){
        super(uri, raftOptions);

    }
    @Override
    public boolean setFilterBeforeCopyRemote() {
        System.out.println("SnapshotStorage setFilterBeforeCopyRemote");
        return false;
    }

    @Override
    public SnapshotWriter create() {
        System.out.println("SnapshotStorage create");
        return new MySnapshotWriter(this);

    }

    @Override
    public SnapshotReader open() {
        System.out.println("SnapshotStorage open");
        if ( snapshotMeta != null)
            return new MySnapshotReader(this);
        return null;
    }

    @Override
    public SnapshotReader copyFrom(String uri, SnapshotCopierOptions opts) {
        System.out.println("SnapshotStorage copyFrom");
        if ( snapshotMeta != null)
            return new MySnapshotReader(this);
        return null;
    }

    @Override
    public SnapshotCopier startToCopyFrom(String uri, SnapshotCopierOptions opts) {
        System.out.println("SnapshotStorage startToCopyFrom");
        return new MySnapshotCopier();
    }

    @Override
    public void setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {
        System.out.println("SnapshotStorage setSnapshotThrottle");

    }

    @Override
    public boolean init(Void opts) {
        System.out.println("SnapshotStorage init");
        return true;
    }

    @Override
    public void shutdown() {

    }

    static class MySnapshotReader extends SnapshotReader{
        SnapshotStorageImpl storage;
        public MySnapshotReader(SnapshotStorageImpl storage){
            this.storage = storage;
        }

        @Override
        public RaftOutter.SnapshotMeta load() {
            return this.storage.snapshotMeta;
        }

        @Override
        public String generateURIForCopy() {
            return "generateURIForCopy";
        }

        @Override
        public boolean init(Void opts) {
            return true;
        }

        @Override
        public void shutdown() {

        }

        @Override
        public String getPath() {
            System.out.println("GetPath");
            return "null";
        }

        @Override
        public Set<String> listFiles() {
            Set<String> set = new HashSet<>();
            set.add("d:/1.txt");
            return set;
        }

        @Override
        public Message getFileMeta(String fileName) {
            return null;
        }

        @Override
        public void close() throws IOException {

        }
    }

    static class MySnapshotCopier extends SnapshotCopier {

        @Override
        public void cancel() {

        }

        @Override
        public void join() throws InterruptedException {

        }

        @Override
        public void start() {

        }

        @Override
        public SnapshotReader getReader() {
            return null;
        }

        @Override
        public void close() throws IOException {

        }
    }

    static class MySnapshotWriter extends  SnapshotWriter{
        SnapshotStorageImpl storage;
        public MySnapshotWriter(SnapshotStorageImpl storage){
            this.storage = storage;
        }
        @Override
        public boolean saveMeta(RaftOutter.SnapshotMeta meta) {
            storage.snapshotMeta = meta;
            return true;
        }

        @Override
        public boolean addFile(String fileName, Message fileMeta) {
            return true;
        }

        @Override
        public boolean removeFile(String fileName) {
            return false;
        }

        @Override
        public void close(boolean keepDataOnError) throws IOException {

        }

        @Override
        public boolean init(Void opts) {
            return true;
        }

        @Override
        public void shutdown() {

        }

        @Override
        public String getPath() {
            return null;
        }

        @Override
        public Set<String> listFiles() {
            return null;
        }

        @Override
        public Message getFileMeta(String fileName) {
            return null;
        }

        @Override
        public void close() throws IOException {

        }
    }
}
