package org.example;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;
import io.grpc.netty.shaded.io.netty.buffer.ByteBufInputStream;
import org.apache.commons.io.FileUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicLong;

public class StateMachineImpl extends StateMachineAdapter {
    private final AtomicLong leaderTerm = new AtomicLong(-1);

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    private RaftEngine raftEngine;
    private String groupId;
    private Node node;

    public StateMachineImpl(String groupId, final RaftEngine engine) {
        this.groupId = groupId;
        this.raftEngine = engine;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    @Override
    public void onApply(Iterator iterator) {
        // 接收数据
        while (iterator.hasNext()) {
            StoreClosure closure = null;
            if (iterator.done() != null) {
                closure = (StoreClosure) iterator.done();
                Operation op = closure.getOperation();
                System.out.println(groupId + " leader receive data " + op.getValue().length);
                // 本地数据
            } else {
                // 远端数据
                ObjectInputStream input = null;
                try {
                    input = new ObjectInputStream(new ByteArrayInputStream(iterator.getData().array()));
                    Operation op = (Operation) input.readObject();
                    System.out.println(groupId + " follower receive data " + op.getValue().length);
                 //   Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (input != null) {
                        try {
                            input.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            // 更新后，确保调用 done，返回应答给客户端。
            if (closure != null) {
                closure.run(Status.OK());
            }
            iterator.next();
        }
    }

    @Override
    public void onError(final RaftException e) {
        System.out.println(groupId + " raft error " + e.getStatus());
        e.printStackTrace();
        Utils.runInThread(() -> {
            raftEngine.restartRaftNode(groupId);
        });

    }

    @Override
    public void onLeaderStart(final long term) {
        this.leaderTerm.set(term);
        super.onLeaderStart(term);
        System.out.println(groupId + "  is leader");
    }

    @Override
    public void onLeaderStop(final Status status) {
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
        System.out.println(groupId + "  lose leader " + status);
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        String filePath = writer.getPath() + File.separator + "snapshot";
        try {
            FileUtils.writeStringToFile(new File(filePath), groupId + "snapshot", Charset.defaultCharset());
            if (writer.addFile("snapshot")) {
                System.out.println(groupId + " snapshot save");
                done.run(Status.OK());
            } else {
                done.run(new Status(RaftError.EIO, "Fail to add file to writer"));
            }
        } catch (IOException e) {
            e.printStackTrace();
            done.run(new Status(RaftError.EIO, "Fail to save counter snapshot %s", filePath));
        }
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            System.out.println("Leader is not supposed to load snapshot");
            return false;
        }
        System.out.println("onSnapshotLoad ");
        reader.listFiles().forEach(f->{
            System.out.println("\t " + f);
        });
        try {
            FileUtils.forceDelete(new File(reader.getPath()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }


    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        System.out.println(groupId + " stop follower");
    }


    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        System.out.println(groupId + " start follower");
    }
}
