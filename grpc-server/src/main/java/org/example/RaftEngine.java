package org.example;

import com.alipay.sofa.jraft.*;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.Replicator;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import org.apache.commons.lang.StringUtils;
import org.example.rpc.RaftNodeProcessor;
import org.example.rpc.SnapshotProcessor;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RaftEngine {

    private PeerId serverId;
    private RpcServer rpcServer;
    private Map<String, Node> raftNodes = new ConcurrentHashMap<>();
    private String basePath;
    private LogStorageImpl logStorage;
    /**
     * 创建raft rpc server
     */
    public void createRaftRpcServer(String raftAddr) {
        serverId = JRaftUtils.getPeerId(raftAddr);
        rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());
        // 注册增加Raft node消息
        rpcServer.registerProcessor(new RaftNodeProcessor(this));
        SnapshotProcessor.registerProcessor(rpcServer, this);
        rpcServer.init(null);
    }

    /**
     * 创建raft分组
     *
     * @param groupId
     */
    public void startRaftNode(String groupId, String peersList) {
        if (raftNodes.containsKey(groupId))
            return;
        String raftPath = basePath + "/" + groupId;
        new File(raftPath).mkdirs();

        startRaft(groupId, raftPath, peersList);
    }

    /**
     * 创建raft分组
     *
     * @param groupId
     */
    public void stopRaftNode(String groupId) {
        if (!raftNodes.containsKey(groupId))
            return;
        Node node = raftNodes.remove(groupId);
        node.shutdown();

    }

    /**
     * 创建raft分组
     *
     * @param dataPath  存储路
     * @param peersList 集群地址
     */
    public void startRaft(String groupId, String dataPath, String peersList) {
        System.out.println("Start raft " + groupId + " peers " + peersList);
        String logPath = dataPath + "/log/" + groupId;
        String metaPath = dataPath + "/meta/" + groupId;
        String snapPath = dataPath + "/snapshot/" + groupId;
        new File(logPath).mkdirs();
        new File(metaPath).mkdirs();
        new File(snapPath).mkdirs();
        // 创建状态机
        StateMachineImpl stateMachine = new StateMachineImpl(groupId);
        Configuration initConf = new Configuration();
        initConf.parse(peersList);
        // 设置Node参数，包括日志存储路径和状态机实例
        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setFsm(stateMachine);
        // 日志路径
        nodeOptions.setLogUri(logPath);
        // raft元数据路径
        nodeOptions.setRaftMetaUri(metaPath);
        // 快照路径
        nodeOptions.setSnapshotUri(snapPath);
        // 初始集群
        nodeOptions.setInitialConf(initConf);
        // 快照时间间隔
        nodeOptions.setSnapshotIntervalSecs(10000);

/*
        nodeOptions.setServiceFactory(new DefaultJRaftServiceFactory(){

            @Override
            public SnapshotStorage createSnapshotStorage(final String uri, final RaftOptions raftOptions) {
                return new SnapshotStorageImpl(uri, raftOptions);
            }

            @Override
            public LogStorage createLogStorage(final String uri, final RaftOptions raftOptions) {
                logStorage = new LogStorageImpl(uri, raftOptions);
                return logStorage;
            }
        });
 */

        // 构建raft组并启动raft
        RaftGroupService raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer, true);
        Node raftNode = raftGroupService.start(false);
        raftNode.addReplicatorStateListener(new Replicator.ReplicatorStateListener() {
            @Override
            public void onCreated(PeerId peer) {
                System.out.println("Replicator onCreated ");
            }

            @Override
            public void onError(PeerId peer, Status status) {
                System.out.println("Replicator onError ");
            }

            @Override
            public void onDestroyed(PeerId peer) {
                System.out.println("Replicator onDestroyed ");
            }
        });

        stateMachine.setNode(raftNode);
        raftNodes.put(groupId, raftNode);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            raftNode.shutdown();
        }));
        //    NodeOptions ops = raftNode.getOptions();
        System.out.println("OK");

    }
    public Node getRaftNode(String groupId) throws Exception {
        if (!raftNodes.containsKey(groupId))
            throw new Exception("Do not found " + groupId);
        return raftNodes.get(groupId);
    }

    public void addPeer(String groupId, String peer, final Closure done) throws Exception {
        System.out.println(groupId + " addPeer " + peer);
        Node node = getRaftNode(groupId);
        PeerId peerId = JRaftUtils.getPeerId(peer);
        node.addPeer(peerId, done);
    }

    public void removePeer(String groupId, String peer, final Closure done) throws Exception {
        System.out.println(groupId + " removePeer " + peer);
        Node node = getRaftNode(groupId);
        PeerId peerId = JRaftUtils.getPeerId(peer);
        node.removePeer(peerId, done);
    }

    public void changePeers(String groupId, String follower, String learner, final Closure done) throws Exception {
        System.out.println(groupId + " changePeers " + follower + " learner " + learner);
        Node node = getRaftNode(groupId);

        Configuration newPeers = new Configuration();
        for(String peer : follower.split(";")){
            newPeers.addPeer(JRaftUtils.getPeerId(peer));
        }
        for(String peer : learner.split(";")){
            if (StringUtils.isNotBlank(peer))
                newPeers.addLearner(JRaftUtils.getPeerId(peer));
        }
        node.changePeers(newPeers, done);
    }

    /**
     * 是否开启raft log日志
     */
    public boolean setRaftLogMode(String groupId, boolean enable) throws Exception {
        // 检查peer数量，只有单副本才能关闭log
//        if (!enable || getPeers(groupId).size() == 1) {
//            Node node = getRaftNode(groupId);
//            logStorage.setLogMode(enable);
//        }
        return false;
    }


    /**
     * 获取Leader
     */
    public PeerId getLeader(String groupId) throws Exception {
        return getRaftNode(groupId).getLeaderId();
    }



    public List<PeerId> getPeers(String groupId) throws Exception {
        Node node = getRaftNode(groupId);
        return node.listPeers();
    }
    /**
     * 当前peer是否是leader
     */
    public boolean isLeader(String groupId) {
        try {
            return getRaftNode(groupId).isLeader();
        } catch (Exception e) {
            return false;
        }
    }

    public void setBasePath(String basePath){
        this.basePath = basePath;
    }



    public SnapshotProcessor.Status getSnapshotFile(SnapshotProcessor.GetSnapshotRequest request){
        System.out.println("getSnapshotFile request " + request);
        return SnapshotProcessor.Status.OK;
    }

    public SnapshotProcessor.Status receiveSnapshotFile(SnapshotProcessor.TransSnapshotRequest request){
        String s = "";
        List<byte[]> data = request.getData();
        for(byte[] v : data){
            s += new String(v);
            s += " ";
        }
        System.out.println("receiveSnapshotFile request " + request);
        System.out.println("receiveSnapshotFile " + s);
        return SnapshotProcessor.Status.OK;
    }
}
