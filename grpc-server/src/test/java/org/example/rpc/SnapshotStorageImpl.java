package org.example.rpc;

import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotStorage;

public class SnapshotStorageImpl extends LocalSnapshotStorage {
    public SnapshotStorageImpl(String path, RaftOptions raftOptions) {
        super(path, raftOptions);
    }
}
