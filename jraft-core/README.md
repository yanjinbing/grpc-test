# SOFAJRaft
克隆自 https://github.com/sofastack/sofa-jraft/tree/1.3.9.fix-log4j2

## 说明

NodeImpl.applyQueue、LogManagerImpl.diskQueue、FSMCallerImpl.taskQueue 存在queue overload问题。
queue设置太长，在同时启动多个Raft Group时存在OutOfMemory的问题。
为了解决overload、outOfMemory问题，对Queue使用进行监控，当Queue可用空间小于阈值时，通知leader进行限速。

