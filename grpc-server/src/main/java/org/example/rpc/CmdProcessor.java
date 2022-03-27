package org.example.rpc;

import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.rpc.RpcServer;
import lombok.Data;
import org.example.RaftEngine;

import java.io.Serializable;
import java.util.List;

/**
 * 快照同步rpc处理器，leader批量入库完成后，基于seqnum读取新增的kv,批量发送给follower.
 * @param <T>
 */
public class CmdProcessor<T extends CmdProcessor.BaseRequest> implements RpcProcessor<T> {

    public static void registerProcessor(final RpcServer rpcServer, final RaftEngine engine){
        rpcServer.registerProcessor(new CmdProcessor<>(GetSnapshotRequest.class, engine));
        rpcServer.registerProcessor(new CmdProcessor<>(TransSnapshotRequest.class, engine));
        rpcServer.registerProcessor(new CmdProcessor<>(InstallSnapshotRequest.class, engine));
        rpcServer.registerProcessor(new CmdProcessor<>(InstallSnapshotOKRequest.class, engine));
    }

    private final Class<?> requestClass;
    private final RaftEngine engine;

    public CmdProcessor(Class<?> requestClass, RaftEngine engine) {
        this.requestClass = requestClass;
        this.engine = engine;
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, T request) {
        switch (request.magic()) {
            case BaseRequest.GET_SNAPSHOT: {
                Status status = engine.getSnapshotFile((GetSnapshotRequest) request);
                GetSnapshotResponse response = new GetSnapshotResponse();
                response.setSeqNum(100);
                response.setStatus(status);
                rpcCtx.sendResponse(response);
                break;
            }
            case BaseRequest.TRANS_SNAPSHOT: {
                Status status = engine.receiveSnapshotFile((TransSnapshotRequest) request);
                TransSnapshotResponse response = new TransSnapshotResponse();
                response.setStatus(status);
                rpcCtx.sendResponse(response);
                break;
            }
            case BaseRequest.INSTALL_SNAPSHOT:{
                Status status = engine.installSnapshot((InstallSnapshotRequest) request);
                InstallSnapshotResponse response = new InstallSnapshotResponse();
                response.setStatus(status);
                rpcCtx.sendResponse(response);
                break;
            }
            case BaseRequest.INSTALL_SNAPSHOT_OK:{
                Status status = engine.installSnapshotOK((InstallSnapshotOKRequest) request);
                InstallSnapshotOKResponse response = new InstallSnapshotOKResponse();
                response.setStatus(status);
                rpcCtx.sendResponse(response);
                break;
            }
            default:
        }
    }

    @Override
    public String interest() {
        return this.requestClass.getName();
    }

    public abstract static class BaseRequest implements Serializable {
        public static final byte GET_SNAPSHOT = 0x01;
        public static final byte TRANS_SNAPSHOT = 0x02;
        public static final byte REPORT_HEALTHY = 0x03;
        public static final byte INSTALL_SNAPSHOT = 0x04;
        public static final byte INSTALL_SNAPSHOT_OK = 0x05;

        public abstract byte magic();
    }

    @Data
    public abstract static class BaseResponse implements Serializable {
        private Status status;

    }

    /**
     * Follower发起的获取文件请求
     */
    @Data
    public static class GetSnapshotRequest extends BaseRequest {
        private String graphName;
        private int partitionId;
        private long seqNum;        // 开始seqnum

        @Override
        public byte magic() {
            return GET_SNAPSHOT;
        }
    }

    @Data
    public static class GetSnapshotResponse extends BaseResponse {
        private long seqNum;    // 最新的seqnum
    }

    /**
     * Follower发起的获取文件请求
     */
    @Data
    public static class InstallSnapshotRequest extends BaseRequest {
        private String graphName;
        private int partitionId;
        private String uri;        // 开始seqnum

        @Override
        public byte magic() {
            return INSTALL_SNAPSHOT;
        }
    }

    @Data
    public static class InstallSnapshotResponse extends BaseResponse {
        private long seqNum;    // 最新的seqnum
    }

    /**
     * Follower发起的获取文件请求
     */
    @Data
    public static class InstallSnapshotOKRequest extends BaseRequest {
        private String graphName;
        private int partitionId;
        private String uri;        // 开始seqnum

        @Override
        public byte magic() {
            return INSTALL_SNAPSHOT_OK;
        }
    }

    @Data
    public static class InstallSnapshotOKResponse extends BaseResponse {
        private long seqNum;    // 最新的seqnum
    }

    /**
     * Leader发起的传输文件请求
     */
    @Data
    public static class TransSnapshotRequest extends BaseRequest {
        private String graphName;
        private int partitionId;
        private long startSeqNum;
        private long endSeqNum;
        private List<byte[]> data;           // 数据内容
        private Status  status;         // 传输状态,COMPLETE传输完成，INCOMPLETE未传输完成，还需要继续传输

        @Override
        public byte magic() {
            return TRANS_SNAPSHOT;
        }
    }

    @Data
    public static class TransSnapshotResponse extends BaseResponse {
    }

    @Data
    public static class HealthyRequest extends BaseRequest{
        private int level;

        @Override
        public byte magic() {
            return REPORT_HEALTHY;
        }
    }

    @Data
    public static class HealthyResponse extends BaseResponse{
        private int level;


    }

    public enum Status implements Serializable{
        UNKNOWN(-1, "unknown"),
        OK(0, "ok"),
        COMPLETE(0, "Transmission completed"),
        INCOMPLETE(1, "Incomplete transmission"),
        NO_PARTITION(10, "Partition not found"),
        IO_ERROR(11, "io error"),
        ABORT(100, "Transmission aborted");

        private final int code;
        private final String msg;

        Status(int code, String msg) {
            this.code = code;
            this.msg = msg;
        }
    }
}
