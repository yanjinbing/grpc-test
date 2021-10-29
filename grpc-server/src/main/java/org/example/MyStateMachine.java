package org.example;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftException;
import io.grpc.netty.shaded.io.netty.buffer.ByteBufInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.atomic.AtomicLong;

public class MyStateMachine extends StateMachineAdapter {
    private final AtomicLong leaderTerm = new AtomicLong(-1);

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    @Override
    public void onApply(Iterator iterator) {
        // 接收数据
        while (iterator.hasNext()) {
            StoreClosure closure = null;
            if (iterator.done() != null) {
                closure = (StoreClosure) iterator.done();
                Operation op = closure.getOperation();
                System.out.println("本地收到 " + new String(op.getKey()));
                // 本地数据
            } else {
                // 远端数据
                ObjectInputStream input = null;
                try {
                    input = new ObjectInputStream(new ByteArrayInputStream(iterator.getData().array()));
                    Operation op = (Operation) input.readObject();
                    System.out.println("远端收到 " + new String(op.getKey()));
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
            iterator.next();
        }
        System.out.println("raft收到数据");
    }

    @Override
    public void onError(final RaftException e) {
        System.out.println("发生错误 " + e.getStatus());
        e.printStackTrace();
    }

    @Override
    public void onLeaderStart(final long term) {
        this.leaderTerm.set(term);
        super.onLeaderStart(term);
        System.out.println("成为leader");
    }

    @Override
    public void onLeaderStop(final Status status) {
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
        System.out.println("失去leader " + status);
    }

}
