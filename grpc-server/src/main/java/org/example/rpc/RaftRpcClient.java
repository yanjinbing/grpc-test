package org.example.rpc;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.InvokeTimeoutException;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.*;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.impl.AbstractClientService;
import com.alipay.sofa.jraft.rpc.impl.FutureImpl;
import com.alipay.sofa.jraft.util.Endpoint;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * raft node之间发消息，包括创建node，复制快照等
 */
public class RaftRpcClient extends AbstractClientService {
    private RpcOptions cliOptions;

    public static abstract class ClosureAdapter<T> implements Closure {
        private T resp;

        public T getResponse() {
            return this.resp;
        }

        public void setResponse(T resp) {
            this.resp = resp;
        }
    }

    @Override
    public synchronized boolean init(final RpcOptions rpcOptions) {
        boolean ret = super.init(rpcOptions);
        if (ret) {
            this.cliOptions = this.rpcOptions;
        }
        return ret;
    }


    public <T> Future<T> addRaftNode(final Endpoint endpoint, final RaftNodeProcessor.Request request,
                                     final ClosureAdapter<RaftNodeProcessor.Response> done) {
        return (Future<T>) invokeWithDone(endpoint, request, done, this.cliOptions.getRpcDefaultTimeout(), null);
    }


    public <T> Future<T> invokeWithDone(final Endpoint endpoint, final Object request,
                                        final ClosureAdapter<T> done, final int timeoutMs) {
        return invokeWithDone(endpoint, request, done, timeoutMs, null);

    }

    public <T> Future<T> invokeWithDone(final Endpoint endpoint, final Object request,
                                        final ClosureAdapter<T> done, final int timeoutMs,
                                        final Executor rpcExecutor) {
        final RpcClient rc = this.rpcClient;
        final InvokeContext ctx = null;
        final FutureImpl<T> future = new FutureImpl<>();
        final Executor currExecutor = rpcExecutor != null ? rpcExecutor : this.rpcExecutor;
        try {
            if (rc == null) {
                future.failure(new IllegalStateException("Client service is uninitialized."));
                // should be in another thread to avoid dead locking.
                RpcUtils.runClosureInExecutor(currExecutor, done, new Status(RaftError.EINTERNAL,
                        "Client service is uninitialized."));
                return future;
            }
            rc.invokeAsync(endpoint, request, ctx, new InvokeCallback() {
                @Override
                public void complete(Object result, Throwable err) {
                    if (future.isCancelled()) {
                        onCanceled(request, done);
                        return;
                    }
                    if (err == null) {
                        Status status = Status.OK();
                        Object msg;
                        if (result instanceof RpcRequests.ErrorResponse) {
                            status = handleErrorResponse((RpcRequests.ErrorResponse) result);
                            msg = (Message) result;
                        } else if (result instanceof Message) {
                            final Descriptors.FieldDescriptor fd = ((Message) result).getDescriptorForType() //
                                    .findFieldByNumber(RpcResponseFactory.ERROR_RESPONSE_NUM);
                            if (fd != null && ((Message) result).hasField(fd)) {
                                final RpcRequests.ErrorResponse eResp = (RpcRequests.ErrorResponse) ((Message) result).getField(fd);
                                status = handleErrorResponse(eResp);
                                msg = eResp;
                            } else {
                                msg = (T) result;
                            }
                        } else {
                            msg = (T) result;
                        }
                        if (done != null) {
                            try {
                                if (status.isOk()) {
                                    done.setResponse((T) msg);
                                }
                                done.run(status);
                            } catch (final Throwable t) {
                                LOG.error("Fail to run RpcResponseClosure, the request is {}.", request, t);
                            }
                        }
                        if (!future.isDone()) {
                            future.setResult((T) msg);
                        }
                    } else {
                        if (done != null) {
                            try {
                                done.run(new Status(err instanceof InvokeTimeoutException ? RaftError.ETIMEDOUT
                                        : RaftError.EINTERNAL, "RPC exception:" + err.getMessage()));
                            } catch (final Throwable t) {
                                LOG.error("Fail to run RpcResponseClosure, the request is {}.", request, t);
                            }
                        }
                        if (!future.isDone()) {
                            future.failure(err);
                        }
                    }
                }
            }, timeoutMs);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            future.failure(e);
            // should be in another thread to avoid dead locking.
            RpcUtils.runClosureInExecutor(currExecutor, done,
                    new Status(RaftError.EINTR, "Sending rpc was interrupted"));
        } catch (final RemotingException e) {
            future.failure(e);
            // should be in another thread to avoid dead locking.
            RpcUtils.runClosureInExecutor(currExecutor, done, new Status(RaftError.EINTERNAL,
                    "Fail to send a RPC request:" + e.getMessage()));
        }
        return future;
    }

    private static Status handleErrorResponse(final RpcRequests.ErrorResponse eResp) {
        final Status status = new Status();
        status.setCode(eResp.getErrorCode());
        if (eResp.hasErrorMsg()) {
            status.setErrorMsg(eResp.getErrorMsg());
        }
        return status;
    }

    private <T> void onCanceled(final Object request, final ClosureAdapter<T> done) {
        if (done != null) {
            try {
                done.run(new Status(RaftError.ECANCELED, "RPC request was canceled by future."));
            } catch (final Throwable t) {
                LOG.error("Fail to run RpcResponseClosure, the request is {}.", request, t);
            }
        }
    }
}
