package org.example;

import com.alipay.sofa.jraft.*;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.core.Replicator;
import com.alipay.sofa.jraft.core.TimerManager;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.option.SnapshotCopierOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotCopier;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotStorage;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Utils;
import org.apache.commons.lang.StringUtils;
import org.example.rpc.CmdClient;
import org.example.rpc.RaftNodeProcessor;
import org.example.rpc.CmdProcessor;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class RaftEngine {

    private PeerId serverId;
    private RpcServer rpcServer;
    private Map<String, Node> raftNodes = new ConcurrentHashMap<>();
    private Map<String, RaftMonitor> raftMonitors = new ConcurrentHashMap<>();
    private String basePath;
    private LogStorageImpl logStorage;
    private ScheduledExecutorService executor;
    private CmdClient cmdClient;

    public RaftEngine() {
        this.executor = Executors.newScheduledThreadPool(2);
        executor.scheduleWithFixedDelay(() -> {
            raftMonitors.forEach((groupId, monitor)->{
                if ( monitor.getLevel() != monitor.getLastLevel()) {
                    System.out.println(monitor.toString());
                }

            });
        }, 1, 1, TimeUnit.SECONDS);


        cmdClient = new CmdClient();
        cmdClient.init(new RpcOptions());
    }
    /**
     * 创建raft rpc server
     */
    public void createRaftRpcServer(String raftAddr) {
        serverId = JRaftUtils.getPeerId(raftAddr);
        rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint(),
                JRaftUtils.createExecutor("RAFT-RPC-", Utils.cpus() * 6),null);
        // 注册增加Raft node消息
        rpcServer.registerProcessor(new RaftNodeProcessor(this));
        CmdProcessor.registerProcessor(rpcServer, this);
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

    public void restartRaftNode(String groupId) {
        if (!raftNodes.containsKey(groupId))
            return;
        Node node = raftNodes.remove(groupId);

        System.out.println("Node is active " + node.getNodeState().isActive());
        NodeOptions options = node.getOptions();
      //  options.getRaftOptions().setDisruptorBufferSize(1024);
        node.shutdown();
        try {
            node.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        startRaft(groupId, options);

    }


    /**
     * 创建raft分组
     *
     * @param dataPath  存储路
     * @param peersList 集群地址
     */
    public void startRaft(String groupId, String dataPath, String peersList) {
        System.out.println("Start raft " + groupId + " peers " + peersList + " " + dataPath);
        String logPath = dataPath + "/log/" + groupId;
        String metaPath = dataPath + "/meta/" + groupId;
        String snapPath = dataPath + "/snapshot/" + groupId;
        new File(logPath).mkdirs();
        new File(metaPath).mkdirs();
        new File(snapPath).mkdirs();
        // 创建状态机
        StateMachineImpl stateMachine = new StateMachineImpl(groupId, this);
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
        nodeOptions.setElectionTimeoutMs(10000);
        // 快照时间间隔
   //     nodeOptions.setSnapshotIntervalSecs(10);
        nodeOptions.setSharedVoteTimer(true);
        nodeOptions.setSharedStepDownTimer(true);
        nodeOptions.setSharedSnapshotTimer(true);
        nodeOptions.setSharedElectionTimer(true);
        nodeOptions.setSharedTimerPool(true);
        nodeOptions.setElectionPriority(serverId.getPriority());
        nodeOptions.setRpcDefaultTimeout(5000);
        nodeOptions.setEnableMetrics(true);
        RaftOptions raftOptions = nodeOptions.getRaftOptions();
        raftOptions.setDisruptorBufferSize(4096);

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
        startRaft(groupId, nodeOptions);
    }

    public void startRaft(String groupId, NodeOptions nodeOptions){
        System.out.println("Start raft " + groupId + " options " + nodeOptions);

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

            }

            @Override
            public void onDestroyed(PeerId peer) {
                System.out.println("Replicator onDestroyed ");
            }
        });

        ((StateMachineImpl)nodeOptions.getFsm()).setNode(raftNode);
        raftNodes.put(groupId, raftNode);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            raftNode.shutdown();
        }));

        raftMonitors.put(groupId, new RaftMonitor(
                raftNode.getNodeMetrics().getMetricRegistry(), 1));


        Status status = raftNode.resetPeers(nodeOptions.getInitialConf());
        if ( !status.isOk())
            raftNode.changePeers(nodeOptions.getInitialConf(), status1 -> {
                System.out.println("changepeers");
            });
        //    NodeOptions ops = raftNode.getOptions();
        System.out.println("Start raft OK!!!");

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

    public void transferLeader(String groupId, String peer) throws Exception {
        Node node = getRaftNode(groupId);
        node.transferLeadershipTo(JRaftUtils.getPeerId(peer));
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
        Node node = getRaftNode(groupId);
        System.out.println("Node state is " + node.getNodeState());

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



    public CmdProcessor.Status getSnapshotFile(CmdProcessor.GetSnapshotRequest request){
        System.out.println("getSnapshotFile request " + request);
        return CmdProcessor.Status.OK;
    }

    public CmdProcessor.Status receiveSnapshotFile(CmdProcessor.TransSnapshotRequest request){
        String s = "";
        List<byte[]> data = request.getData();
        for(byte[] v : data){
            s += new String(v);
            s += " ";
        }
        System.out.println("receiveSnapshotFile request " + request);
        System.out.println("receiveSnapshotFile " + s);
        return CmdProcessor.Status.OK;
    }

    public CmdProcessor.Status installSnapshot(CmdProcessor.InstallSnapshotRequest request){
        try {
            System.out.println("Receive install snapshot " + request);
            receiveSnapshot(request.getGraphName(), request.getUri());
            CmdClient client = new CmdClient();
            CmdProcessor.InstallSnapshotOKRequest request2 = new CmdProcessor.InstallSnapshotOKRequest();
           // client.installSnapshotOK(request2);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return CmdProcessor.Status.OK;
    }

    public CmdProcessor.Status installSnapshotOK(CmdProcessor.InstallSnapshotOKRequest request){
        return CmdProcessor.Status.OK;
    }

    public void createSnapshot(String groupId, long lastIndex) throws Exception {
        String path = this.basePath + "/" + groupId;

        Node node = getRaftNode(groupId);
        Endpoint localAddr = node.getNodeId().getPeerId().getEndpoint();

        final RaftOutter.SnapshotMeta.Builder metaBuilder = RaftOutter.SnapshotMeta.newBuilder() //
                .setLastIncludedIndex(lastIndex) //
                .setLastIncludedTerm(1);

        LocalSnapshotStorage storage = new LocalSnapshotStorage(path, node.getRaftOptions());
        storage.init(null);
        storage.setServerAddr(localAddr);
        SnapshotWriter writer = storage.create();
        writer.saveMeta(metaBuilder.build());

        node.getOptions().getFsm().onSnapshotSave(writer, status -> {
            try {
                System.out.println("save snapshot " + status);
                writer.close();
                SnapshotReader reader = storage.open();
                reader.load();

                CmdProcessor.InstallSnapshotRequest request = new CmdProcessor.InstallSnapshotRequest();
                request.setGraphName(groupId);
                request.setUri(reader.generateURIForCopy());
                getPeers(groupId).forEach(peerId -> {
                    if (!peerId.getEndpoint().equals(localAddr)) {
                        System.out.println("Send install snapshot " + peerId.getEndpoint());
                        try {
                            cmdClient.installSnapshot(peerId.getEndpoint().toString(), request).get();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
       // writer.addFile("file.txt");
    }

    public void receiveSnapshot(String groupId, String uri) throws Exception {
        String path = this.basePath + "/" + groupId;
        Node node = getRaftNode(groupId);
        SnapshotStorageImpl storage = new SnapshotStorageImpl(path, node.getRaftOptions());

        final LocalSnapshotCopier copier = new LocalSnapshotCopier();
        copier.setStorage(storage);
        copier.setSnapshotThrottle(null);
        copier.setFilterBeforeCopyRemote(true);
        if (!copier.init(uri, newCopierOpts((NodeImpl) node))) {
            System.out.println("Error");
        }
        copier.start();
        copier.join();
        if (copier.getCode() != 0)
            System.out.println("copier error " + copier.getErrorMsg());
        copier.close();
        if (copier.getCode() != 0)
            System.out.println("copier close error " + copier.getErrorMsg());
        SnapshotReader reader = copier.getReader();
        reader.listFiles().forEach(s -> {
            System.out.println(s);
        });
        reader.close();
    }

    private SnapshotCopierOptions newCopierOpts(NodeImpl node) {
        final SnapshotCopierOptions copierOpts = new SnapshotCopierOptions();
        copierOpts.setNodeOptions(node.getOptions());
        copierOpts.setRaftClientService(node.getRpcService());
        copierOpts.setTimerManager(node.getTimerManager());
        copierOpts.setRaftOptions(node.getRaftOptions());
        return copierOpts;
    }
}
