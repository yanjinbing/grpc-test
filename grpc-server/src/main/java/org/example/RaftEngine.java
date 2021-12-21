package org.example;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.DefaultJRaftServiceFactory;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import org.example.rpc.RaftNodeProcessor;

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
        nodeOptions.setSnapshotIntervalSecs(10);


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
        // 构建raft组并启动raft
        RaftGroupService raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer, true);
        Node raftNode = raftGroupService.start(false);
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

    /**
     * 是否开启raft log日志
     */
    public boolean setRaftLogMode(String groupId, boolean enable) throws Exception {
        // 检查peer数量，只有单副本才能关闭log
        if (!enable || getPeers(groupId).size() == 1) {
            Node node = getRaftNode(groupId);
            logStorage.setLogMode(enable);
        }
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


}
