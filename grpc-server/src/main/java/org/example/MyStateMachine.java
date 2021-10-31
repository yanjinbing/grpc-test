package org.example;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import io.grpc.netty.shaded.io.netty.buffer.ByteBufInputStream;
import org.apache.commons.io.FileUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicLong;

public class MyStateMachine extends StateMachineAdapter {
    private final AtomicLong leaderTerm = new AtomicLong(-1);

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }


    private final AtomicLong counter = new AtomicLong(-1);
    private long startTime = System.currentTimeMillis();
    private static long recvData = 0;
    private String groupId;
    public MyStateMachine(String groupId){
        this.groupId = groupId;
    }

    @Override
    public void onApply(Iterator iterator) {
        // 接收数据
        while (iterator.hasNext()) {
            StoreClosure closure = null;
            if (iterator.done() != null) {
                closure = (StoreClosure) iterator.done();
                Operation op = closure.getOperation();
                recvData += op.getValue().length;

                // 本地数据
            } else {
                // 远端数据
                ObjectInputStream input = null;
                try {
                    input = new ObjectInputStream(new ByteArrayInputStream(iterator.getData().array()));
                    Operation op = (Operation) input.readObject();
                    recvData += op.getValue().length;

                } catch (IOException | ClassNotFoundException e) {
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
            long c = counter.incrementAndGet();
            if ( System.currentTimeMillis() - startTime > 1000*10) {
                System.out.println(String.format(groupId + " receive data : %d, size is %d K",
                        c, recvData / (System.currentTimeMillis() - startTime)));
                startTime = System.currentTimeMillis();
                recvData = 0;
            }
            iterator.next();
        }

    }

    @Override
    public void onError(final RaftException e) {
        System.out.println(groupId + " error " + e.getStatus());
        e.printStackTrace();
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
            if (writer.addFile("snapshot")){
                System.out.println(groupId + " snapshot save");
                done.run(Status.OK());
            }else{
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
        String filePath = reader.getPath() + File.separator + "snapshot";
        try {
            String value = FileUtils.readFileToString(new File(filePath), Charset.defaultCharset());
            System.out.println(groupId + " snapshot load " + value);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

}
