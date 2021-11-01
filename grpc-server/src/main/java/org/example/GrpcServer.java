package org.example;


import com.alipay.sofa.jraft.*;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import io.grpc.stub.StreamObserver;
import org.example.grpc.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 */
public class GrpcServer {
    private Server grpcServer;
    // raft状态机
    private RaftParams raftParams;

    private PeerId  serverId;
    private  RpcServer rpcServer;
    private Map<String, RaftGroup> raftGroups = new HashMap<>();

    private void start(int port) throws IOException {
        // 启动grpc服务
        grpcServer = ServerBuilder.forPort(port)
                .addService(new HelloWorldImpl(this))
                .maxInboundMessageSize(1024*1024*10)
                .maxInboundMetadataSize(1024*1024*10)
                .build()
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                GrpcServer.this.stop();
            }
        });
        createRaftRpcServer(raftParams.raftAddr);
        for(int i = 0; i< raftParams.groupCount; i++){
            startRaft("a" + i, raftParams.dataPath, raftParams.peersList);
        }
    }

    private void stop() {
        if (grpcServer != null) {
            grpcServer.shutdown();
        }
    }

    public Node getRaftNode(String groupId) throws Exception {
        if (!raftGroups.containsKey(groupId))
            throw new Exception("Do not found " + groupId);
        return raftGroups.get(groupId).getRaftNode();
    }

    /**
     * 创建raft rpc server
     */
    private void createRaftRpcServer(String raftAddr){
        serverId = JRaftUtils.getPeerId(raftParams.raftAddr);
        rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());
        rpcServer.init(null);
    }
    /**
     * 创建raft分组
     *
     * @param dataPath  存储路
     * @param peersList 集群地址
     */
    private void startRaft(String groupId, String dataPath, String peersList) {

        String logPath = dataPath + "/log/" + groupId;
        String metaPath = dataPath + "/meta/" + groupId;
        String snapPath = dataPath + "/snapshot/" + groupId;
        new File(logPath).mkdirs();
        new File(metaPath).mkdirs();
        new File(snapPath).mkdirs();
        // 创建状态机
        MyStateMachine stateMachine = new MyStateMachine(groupId);
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
        // 构建raft组并启动raft
        RaftGroupService raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer, true);
        Node raftNode = raftGroupService.start(false);

        raftGroups.put(groupId, new RaftGroup(stateMachine, raftNode, raftGroupService));
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {

                    raftNode.shutdown();
                }));


    }


    public PeerId getLeader(String groupId) throws Exception {
        return  getRaftNode(groupId).getLeaderId();
    }

    public boolean isLeader(String groupId)  {
        try {
            return getRaftNode(groupId).isLeader();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }


    /**
     * 实现grpc服务
     */
    static class HelloWorldImpl extends HelloWorldGrpc.HelloWorldImplBase {
        private GrpcServer server;

        public HelloWorldImpl(GrpcServer server) {
            this.server = server;
        }

        //实现grpc方法
        public void sayHello(HelloRequest request, StreamObserver<HelloReply> observer) {
            ByteString key = request.getKey();
            ByteString value = request.getValue();

            //创建任务，发送给其他peer

           String groupId = request.getGroupId();
           if ( !server.isLeader(groupId)){
               observer.onError(io.grpc.Status.ABORTED.asException());
               observer.onCompleted();
               return;
           }
            try {
                // 提交raft 任务
                putTask(groupId, request.getKey().toByteArray(), request.getValue().toByteArray(),
                        new Closure() {
                            @Override
                            public void run(Status status) {
                                observer.onNext(HelloReply.newBuilder().setMessage("response " + request.getKey()).build());
                                observer.onCompleted();
                            }
                        });
            } catch (Exception e) {
                observer.onError(io.grpc.Status.fromThrowable(e).asException());
                observer.onCompleted();
            }
        }

        /**
         * 查询Leader
         * @param request
         * @param observer
         */
        public void getLeader(GetLeaderRequest request, StreamObserver<GetLeaderReply> observer) {
            String groupId = request.getGroupId();
            try {
                PeerId peerId = server.getLeader(groupId);
                if (peerId != null) {
                    GetLeaderReply reply = GetLeaderReply.newBuilder()
                            .setLeader(peerId.toString())
                            .setGroupId(groupId)
                            .build();
                    observer.onNext(reply);
                } else
                    observer.onError(io.grpc.Status.fromCodeValue(1).asException());
            }catch (Exception e){
                observer.onError(io.grpc.Status.fromThrowable(e).asException());
            }
            observer.onCompleted();
        }

        /**
         * 生成raft任务
         * @param groupId
         * @param key
         * @param value
         * @param done
         * @throws Exception
         */
        protected void putTask(final String groupId, final byte[] key, final byte[] value, Closure done) throws Exception {
            Operation op = Operation.createPut(key, value);
            // 序列化
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(bos);
            os.writeObject(op);
            os.close();

            // 创建并提交任务
            final Task task = new Task();
            task.setData(ByteBuffer.wrap(bos.toByteArray()));
            task.setDone(new StoreClosure(op, done));
            this.server.getRaftNode(groupId).apply(task);
        }
    }

    class RaftGroup{
        private MyStateMachine stateMachine;
        private Node raftNode;
        private RaftGroupService service;
        public RaftGroup(MyStateMachine machine, Node node, RaftGroupService service){
            this.stateMachine = machine;
            this.service = service;
            this.raftNode = node;
        }
        public boolean isLeader() {
            return stateMachine.isLeader();
        }

        public Node getRaftNode(){
            return raftNode;
        }

    }
    static class RaftParams {
        public String dataPath;
        public String raftAddr;
        public String peersList;
        public long groupCount;
        public RaftParams(String dataPath, String raftAddr, String peersList, long groupCount){
            this.dataPath = dataPath;
            this.raftAddr = raftAddr;
            this.peersList = peersList;
            this.groupCount = groupCount;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        if (args.length < 4) {
            System.out.println("Useage : {dataPath} {grpcPort} {raftAddr} {peersList} {groupCount}");
            System.out.println("Example:  /tmp/server1 127.0.0.1:8081 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083 100");
            System.exit(1);
        }

        final String dataPath = args[0];
        final String grpcPort = args[1];
        final String raftAddr = args[2];
        final String peersList = args[3];
        long groupCount = 1;
        if ( args.length > 4 )
            groupCount = Long.valueOf(args[4]);


        System.out.println("Start grpc server raft addr is " + raftAddr + ", grpc port is " + grpcPort);
        final GrpcServer server = new GrpcServer();
        server.raftParams = new RaftParams(dataPath, raftAddr, peersList, groupCount);
        new File(dataPath).mkdirs();
        server.start(Integer.valueOf(grpcPort));
        server.blockUntilShutdown();
    }
}
