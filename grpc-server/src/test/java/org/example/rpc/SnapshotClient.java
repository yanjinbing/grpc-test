package org.example.rpc;

import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.closure.CatchUpClosure;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.core.ReplicatorType;
import com.alipay.sofa.jraft.core.TimerManager;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.*;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.rpc.impl.core.DefaultRaftClientService;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotCopier;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotCopier;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotStorage;
import com.alipay.sofa.jraft.storage.snapshot.remote.RemoteFileCopier;
import com.alipay.sofa.jraft.storage.snapshot.remote.Session;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.alipay.sofa.jraft.util.ThreadId;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class SnapshotClient {
    @Test
    public void testFileClient() throws InterruptedException {

        String uri = "remote://127.0.0.1:8500/455348063962248968";

        RaftClientService raftClientService = new DefaultRaftClientService(
                new ReplicatorGroup(){

                    @Override
                    public void describe(Printer out) {

                    }

                    @Override
                    public boolean init(NodeId nodeId, ReplicatorGroupOptions opts) {
                        return false;
                    }

                    @Override
                    public boolean addReplicator(PeerId peer, ReplicatorType replicatorType, boolean sync) {
                        return false;
                    }

                    @Override
                    public void sendHeartbeat(PeerId peer, RpcResponseClosure<RpcRequests.AppendEntriesResponse> closure) {

                    }

                    @Override
                    public ThreadId getReplicator(PeerId peer) {
                        return null;
                    }

                    @Override
                    public void checkReplicator(PeerId peer, boolean lockNode) {

                    }

                    @Override
                    public void clearFailureReplicators() {

                    }

                    @Override
                    public boolean waitCaughtUp(PeerId peer, long maxMargin, long dueTime, CatchUpClosure done) {
                        return false;
                    }

                    @Override
                    public long getLastRpcSendTimestamp(PeerId peer) {
                        return 0;
                    }

                    @Override
                    public boolean stopAll() {
                        return false;
                    }

                    @Override
                    public boolean stopReplicator(PeerId peer) {
                        return false;
                    }

                    @Override
                    public boolean resetTerm(long newTerm) {
                        return false;
                    }

                    @Override
                    public boolean resetHeartbeatInterval(int newIntervalMs) {
                        return false;
                    }

                    @Override
                    public boolean resetElectionTimeoutInterval(int newIntervalMs) {
                        return false;
                    }

                    @Override
                    public boolean contains(PeerId peer) {
                        return false;
                    }

                    @Override
                    public boolean transferLeadershipTo(PeerId peer, long logIndex) {
                        return false;
                    }

                    @Override
                    public boolean stopTransferLeadership(PeerId peer) {
                        return false;
                    }

                    @Override
                    public ThreadId stopAllAndFindTheNextCandidate(ConfigurationEntry conf) {
                        return null;
                    }

                    @Override
                    public PeerId findTheNextCandidate(ConfigurationEntry conf) {
                        return null;
                    }

                    @Override
                    public List<ThreadId> listReplicators() {
                        return null;
                    }
                }
        );
        raftClientService.init(new NodeOptions());

        SnapshotCopierOptions opts = new SnapshotCopierOptions(raftClientService,
                new TimerManager(5),
                new RaftOptions(), new NodeOptions());
        {
            RemoteFileCopier copier = new RemoteFileCopier();
            copier.init(uri, null, opts);

            ByteBufferCollector metaBuf = ByteBufferCollector.allocate(0);
            Session session = copier.startCopy2IoBuffer("file.txt", metaBuf, null);
            session.join();


            System.out.println(new String(metaBuf.getBuffer().array()));
        }
        {

            final LocalSnapshotCopier copier = new LocalSnapshotCopier();
            copier.setStorage(new LocalSnapshotStorage("D:/test/10", new RaftOptions()){

            });
            copier.setSnapshotThrottle(null);
            copier.setFilterBeforeCopyRemote(true);
            if (!copier.init(uri, opts)) {
                System.out.println("Error");
            }
            copier.start();

            copier.join();
        }
    }


    @Test
    public void testSnapshotClient() throws InterruptedException, IOException {

        String uri = "remote://127.0.0.1:8500/721152290147786144";
        String path = "D:/test/10";

        RaftClientService raftClientService = new DefaultRaftClientService(
                new ReplicatorGroup(){

                    @Override
                    public void describe(Printer out) {

                    }

                    @Override
                    public boolean init(NodeId nodeId, ReplicatorGroupOptions opts) {
                        return false;
                    }

                    @Override
                    public boolean addReplicator(PeerId peer, ReplicatorType replicatorType, boolean sync) {
                        return false;
                    }

                    @Override
                    public void sendHeartbeat(PeerId peer, RpcResponseClosure<RpcRequests.AppendEntriesResponse> closure) {

                    }

                    @Override
                    public ThreadId getReplicator(PeerId peer) {
                        return null;
                    }

                    @Override
                    public void checkReplicator(PeerId peer, boolean lockNode) {

                    }

                    @Override
                    public void clearFailureReplicators() {

                    }

                    @Override
                    public boolean waitCaughtUp(PeerId peer, long maxMargin, long dueTime, CatchUpClosure done) {
                        return false;
                    }

                    @Override
                    public long getLastRpcSendTimestamp(PeerId peer) {
                        return 0;
                    }

                    @Override
                    public boolean stopAll() {
                        return false;
                    }

                    @Override
                    public boolean stopReplicator(PeerId peer) {
                        return false;
                    }

                    @Override
                    public boolean resetTerm(long newTerm) {
                        return false;
                    }

                    @Override
                    public boolean resetHeartbeatInterval(int newIntervalMs) {
                        return false;
                    }

                    @Override
                    public boolean resetElectionTimeoutInterval(int newIntervalMs) {
                        return false;
                    }

                    @Override
                    public boolean contains(PeerId peer) {
                        return false;
                    }

                    @Override
                    public boolean transferLeadershipTo(PeerId peer, long logIndex) {
                        return false;
                    }

                    @Override
                    public boolean stopTransferLeadership(PeerId peer) {
                        return false;
                    }

                    @Override
                    public ThreadId stopAllAndFindTheNextCandidate(ConfigurationEntry conf) {
                        return null;
                    }

                    @Override
                    public PeerId findTheNextCandidate(ConfigurationEntry conf) {
                        return null;
                    }

                    @Override
                    public List<ThreadId> listReplicators() {
                        return null;
                    }
                }
        );
        raftClientService.init(new NodeOptions());

        SnapshotCopierOptions opts = new SnapshotCopierOptions(raftClientService,
                new TimerManager(5),
                new RaftOptions(), new NodeOptions());

        SnapshotStorageImpl storage = new SnapshotStorageImpl(path, new RaftOptions());
        {
            final LocalSnapshotCopier copier = new LocalSnapshotCopier();
            copier.setStorage(storage);
            copier.setSnapshotThrottle(null);

            if (!copier.init(uri, opts)) {
                System.out.println("Error");
            }
            copier.setFilterBeforeCopyRemote(true);
            copier.start();
            copier.join();
            if ( copier.getCode() != 0)
                System.out.println(copier.getErrorMsg());
            copier.close();
            SnapshotReader reader = copier.getReader();
            reader.listFiles().forEach(s->{
                System.out.println(s);
            });
            reader.close();
        }
    }

    @Test
    public void test(){
        Map<String ,Long> dbSessionUsage = new HashMap<>();
        dbSessionUsage.put("3", 3L);
        dbSessionUsage.put("4", 4L);
        dbSessionUsage.put("5", 5L);
        dbSessionUsage.put("1", 1L);
        dbSessionUsage.put("2", 2L);

        List<Map.Entry<String,Long>> list = new CopyOnWriteArrayList<>(dbSessionUsage.entrySet());
        Collections.sort(list,new Comparator<Map.Entry<String,Long>>() {
            //升序排序
            @Override
            public int compare(Map.Entry<String, Long> o1,
                               Map.Entry<String, Long> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        List<Map.Entry<String,Long>> removed = list.subList(3, list.size());

        for(Map.Entry<String,Long> entry : removed) {
            System.out.println(entry.getKey());
        }

        for(Map.Entry<String, Long> e : list){
            list.remove(e);
        }

        Iterator<Map.Entry<String, Long>> iterator = list.iterator();
        while(iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }

        list.forEach(e->{
            System.out.println(e.getKey());

        });

    }
}
