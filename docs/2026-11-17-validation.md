# 2026-11-17 首个调度器版本验证

时间：2026-11-17 清晨。位置：D:/dev/cordis-task-scheduler。意图：将 SACAssetsManager、textureBackerDemo 队列及 s-forge 中断机制归纳为独立可安装的 Cordis 调度插件，验证执行端停止、共享需求和检查点恢复。

## 源码与边界

实现文件：src/scheduler.ts、heap.ts、types.ts、worker.ts、plugin.ts、index.ts。使用严格 TypeScript，无 any。仅 registry 开发依赖和 Cordis peerDependency，没有 file:/link:。原参考源码尚未修改，发布后需在三个队列入口添加迁移注释。

优先级数值越大越高，范围 -100..100；同级 FIFO 或显式 LIFO。分段等待老化后线性重建索引堆，执行保留领取时公平性权重。任务注册实例内按业务 key 去重，每个调用者拥有独立租约。原子分配资源集合；停止请求不提前释放额度。WorkerExecutor 以 terminate 完成回执作为停止边界。检查点由执行器保存，不以队列内存快照冒充外部事务。

s-forge 实际消费链已读：kernel/api/magi.go unifiedDispatcher 串行消费环队列，submitMagiTaskWithReplyStream 先 InterruptHeartbeat 再排前台请求；magi_runtime.go cancel 心跳并等待 done，最多等待 1500ms；分发器仍等待 CoordinateHeartbeat 返回才继续。

## 实际验证

pnpm typecheck 和 pnpm build 成功。未运行任何 mock/生造数据套件，也未新增此类测试文件。

真实素材：Realtek Audio Control 安装资产中的 DTS_HPX_cmyk_4C_rev.jpg（705020字节），及现有思源主题的 oschina_net.png（1527字节）。此轮工作只对真实字节做 SHA-256，不将哈希验证说成颜色准确度验证。

job pwsh-42cabbf32f854b1e exit0：一个 CPU 额度。JPEG 后台启动，PNG 高优先需求到达，同时另一需求订阅同 JPEG；观察到 interrupt(preempt)、resumed、PNG started/succeeded、JPEG attempt2 started/succeeded。同 JPEG 的两个租约 id 相同，结果与主线程独立 readFile/createHash 一致；收尾 used.cpu=0，tasks=0。

job pwsh-b7c298d4b94c4cbc exit0：真实 Cordis Context 装载插件。按1024字节读取 JPEG，保存 offset 与 hash.copy 检查点后提交前台 PNG。观察到从1024偏移恢复，哈希与独立整文件读取一致。另一个 JPEG 租约立即释放不影响保留租约；随后提交新任务并卸载 Cordis，公开结果取消，closed=true、tasks=0、used.disk=0。该卸载检查没有证明一个已经运行很久的原生任务能停止，后续应用验收还要覆盖运行态卸载。

## 已修正的验证问题

pwsh-5c7ac3ad485f4615：验证 Worker 继承 --input-type=module 导致 CommonJS require 失败，改工厂 execArgv=[] 后重跑。不是调度成功证据。

一次同步命令：错误调用 ctx.start，实际 Cordis 无此API；按FileBrowser runtime真实用法改为 ctx.plugin(...).await()。

pwsh-154f0525b3f047ff：检查点验收失败，没有输出分项值，不能断言具体原因。重跑将高优先提交放到首个真实检查点之后，并输出 hashMatched 与 checkpointOffsets，才确认恢复。

源码复查修正：成功结果推迟到释放资源后交付；事件观察回调排入 microtask 防止中途重入；同级 FIFO/LIFO 使用一致比较序；老化改离散步进，避免连续时间戳使 LIFO 永远失效。

## 尚未完成

FileBrowser 未依赖或挂载新库，仍用旧颜色批次和缩略图限制。尚未完成长期公平性、容量驱逐、运行态卸载、超时、暂停恢复和全部生产执行入口验收。颜色版本兼容管理、可见索引需求与占位生命周期分离仍待做。首个预发布只交付已说明边界的独立库，不表示总目标完成。
