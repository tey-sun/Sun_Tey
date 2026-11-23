# gopeed 引擎内部逻辑逆向

范围：下载内核（HTTP/BT/HLS/ed2k/存储/调度）。进程拓扑、REST 路由、扩展引擎、
Flutter UI、绑定层的逆向见 _re/gopeed-reverse-engineering.md 与 _re/02-extension-ui.md
（本 KB 已核对，其结论仍成立）。全部 [S] 证据基于本机 checkout
<研究目录> @7d423e21（2026-10-26）。

## 1. HTTP 多线程引擎：internal/protocol/http/fetcher.go（2610 行）

这是 gopeed 最值钱的一块，也是 Sun_Tey 要「对标 IDM」必须吃透的算法。

### 1.1 关键常量 [S:31-45]

    connectTimeout = 15s        readTimeout = 15s
    minFastFailTimeout = 3s                 快速失败重试下限
    stealThresholdSeconds = 3               只在受害者还需 >3s 完成时才偷块
    stealMinChunkSize = 512*1024            被偷块下限 512KB（避免碎块）
    connReadBufferSize = 64*1024
    错误分类：errRangeRequestIgnored / errInvalidRangeResponse / errIncompleteResponse
              isRangeIntegrityError / isTerminalRangeError（区分「可重试」与「必须回退」）

### 1.2 慢启动扩连：slowStartController [S:157-265]

    状态：maxConnections / totalLaunched / batchPending / batchReady / nextBatchSize / paused
    批大小增长：1 -> 2 -> 4 -> 16 -> 256 -> …（commitBatch 里 nextBatchSize *= nextBatchSize，
      即平方增长；from 1 特判为 2，否则 1*1 永不增长；乘法前先判溢出并 cap 到 maxConnections）
    触发：每条连接拿到 HTTP 响应即 onConnectSuccess()；整批就绪 -> 往 expansionCh 发信号
    失败：onConnectFailed() 递减 batchPending，归零同样触发扩连（避免卡死）
    429/限流：paused=true 时 getNextBatchSize() 返回 0，暂停扩张
    含义：连接数不是「一上来开满」，而是「先证明服务器能吃下更多再翻倍」。
      这是 gopeed 面对国内 CDN/网盘限速不炸的关键，也是 前代实现 现在缺的能力。

### 1.3 分块与再平衡 [S:792-1170, 2030-2078]

    doStart()      等 resolve 完成；错误态重试时若 checksumFailed 则先把文件 truncate(0)
                   再 prepareFullRedownload()（防止半条坏字节残留）
    downloadLoop() isResume = len(connections)>0；
                   新任务：startResolveDownload() 复用 resolve 阶段的响应体继续下（不重复请求）
                   续传：resumeConnections()；有顺序前缀时 commitBatch(1) 先复活它
                   循环等 expansionCh -> checkCompletion / expandConnections / 到 stateSteady 收尾
    首批（expandConnections 第一分支）：
                   若 prefetched >= totalSize 直接完成（先关文件再校验和再发 doneCh）
                   否则首连接从 resolveDataPos 起，newChunk(prefetched, totalSize-1)
                   未知长度用 newOpenEndedChunk(begin)，靠 Content-Range 自己发现总量
    后续批次：不再「均分文件」，而是「帮助最忙的连接」：
                   找 remain 最大且 remain > 2*512KB 的连接，劈开它
    稳态偷块（helpOtherConnection）：
                   按 remain/speed 估每连接剩余秒数；只挑 >3s 的最慢者；
                   helper 拿走其后半段（helper.Begin = end - remain/2），受害者 setEnd(helper.Begin-1)
    意义：下载速度收敛到「木桶最短那块」被不断锯短，尾部不再拖尾。IDM 的分块重下是同一目标的不同实现。

### 1.4 非 Range / 异常路径 [S:1737-1928]

    服务端忽略 Range（206 变 200）-> fallbackToSequentialDownload，单连接顺序流，
      保留续传能力；hasSequentialRecoveryCandidate() 在续传时再探测能否恢复 Range 模式
    未知长度 -> beginUnknownSizeSequentialResource()，边下边 resize
    resolve 预取数据落临时文件（asyncPrefetch/cleanupPrefetchFile），避免二次请求
    校验和：verifyChecksum() 失败 => 截断文件 + 全量重下（不是静默通过）

## 2. BT 引擎：internal/protocol/bt

### 2.1 配置面 config.go [S]

    TrackerSubscribeURLs / AutoUpdateTrackers / CustomTrackers / ListenPort
    SeedKeep / SeedRatio / SeedTime（做种三限）
    DisableDHT / DisablePEX（与 ListenPort 一样：构建共享 client 时读取）

### 2.2 trackers 来源与合并 trackers.go [S]

    内置订阅源（defaultTrackerSubscribeURLs）：
      ngosang/trackerslist 的 trackers_all / _http / _https / _ip / _udp / _ws / _best / _best_ip
      XIU2/TrackersListCollection 的 all / best / http
    validTracker()：scheme 限 udp/http/https/ws/wss，拒绝带 userinfo 的 URL（防劫持）
    uniqueTrackers() + effectiveTrackers()：合并去重但不写回存储（避免配置膨胀）
    state 里记录每个订阅源的抓取状态与 trackerSources 归属（可解释「这些 tracker 从哪来」）

### 2.3 客户端与可观测性 fetcher.go [S]

    单一共享 anacrolix client（initClient：ListenPort、NoDHT、TrackerDialContext 走自定义 DNS 缓存）
    每连接上报来源（tracker / dht）与传输类型（normalizeBTTransport 把 uTP 的 udp socket 归一）
    btPeerRelevance / btPeerCompletion：本地缺要集与远端可用量的匹配度 => 能显示「为什么没速度」
    finishDownload + SeedKeep/Ratio/Time 控制做种；torrentStats() 有 recover（anacrolix#972 会 panic）

## 3. HLS / 视频链路

    internal/protocol/hls/fetcher.go（28KB）+ playlist.go + crypto.go：
      master/variant 解析、EXT-X-KEY、媒体校验（media_validate_test）、staging 临时目录
    ffmpeg 走 pkg/download/engine/ffmpeg/（runtime/spool/input/http_spool）+ inject/ffmpeg 注入到扩展引擎
    与 前代实现 的 hlsx 相比：gopeed 把 ffmpeg 做成可注入运行时（供 JS 扩展调用），
    前代实现 直接 findFFmpeg() + remuxToMP4()。Sun_Tey 取 前代实现 的直连路线 + gopeed 的「后处理钩子」思路。

## 4. 任务与存储层（要点）

    pkg/download/downloader.go（81KB）：Resolve/Create/Start/Pause/Resume/Delete、
      多文件选择、分类（category.go）、解压（extract.go）、webhook、钩子脚本、mirror（镜像/校验和）。
    internal/fetcher/：Fetcher 接口 + controller + jobs（协议可插拔的唯一契约）。
    存储：bbolt 存元数据，状态机含 StatusNeedRestore（重启自动恢复）。
    设计取舍结论（沿用 _re 第十节）：协议可插拔 + 进程内 Dispatch + 重启恢复 = 保留；
      goja/wazero 插件沙箱 + 扩展自带 WebView2 = 砍掉。

---

> 本文从内部研究知识库复制，已抹去本机绝对路径（原研究目录与打包产物路径以 <研究目录>、<前代产物> 代替）。
> 官方原文（gopeed 全站文档全文、OpenAPI、aria2 参数表）见上级目录说明，或 gopeed.com/zh 官网。
