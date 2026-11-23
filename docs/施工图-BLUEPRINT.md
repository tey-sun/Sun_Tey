# Sun_Tey 施工图（BLUEPRINT）

> 本文件是**给 AI 与工程师直接执行的施工合同**，不是愿景介绍。
> 每个里程碑都有：目标 → 输入事实 → 文件级改动 → 验收命令 → 通过判据 → 禁止事项。
> 事实来源：`docs/知识库/`（01–05 号研究文档，入口 `docs/知识库索引.md`）与其中的前代实现体检。
> 证据标记沿用 KB 约定：`[V]`已执行验证 `[S]`源码证据 `[B]`二进制证据 `[I]`推断 `[U]`未验证。

- 产品名：**Sun_Tey**　模块前缀：`suntey`　可执行文件：`Sun_Tey.exe` / `suntey-core.exe`
- 落地目录：`本仓库`　远程：`https://github.com/tey-sun/Sun_Tey`
- 迁移来源：本机已有前代实现（内核与打包已验证）与 `<前代打包产物>`（安装包产物），迁移时按本文件 §3 结构重排。

---

## 0. 一句话产品定义

装完就能用的 Windows 下载器：**HTTP/HTTPS 多线程 + BT + 磁力 + 网页视频嗅探 + 下载后自动转 MP4**，
零配置启动、浏览器自动接管、常用设置集中在一个页面，单个 exe 安装包装完即用。

## 1. 需求 → 里程碑映射（可追踪）

| 编号 | 用户原话需求 | 落地里程碑 | 验收判据（必须真跑） |
|---|---|---|---|
| R1 | IDM 式多线程一定要用 | M1 | 单文件多线程显著快于单线程；慢连接不再拖尾 |
| R2 | BT 下载 | M0/M5 | 公开种子跑通且能选文件 |
| R3 | 磁力链接要能下载（迅雷式） | M2/M5 | 热门磁力 60s 内出元数据并出速度 |
| R4 | 网页插件下载任何视频/格式 | M3 | 直链/HLS/fMP4 三类站可达；DRM 明确拒绝 |
| R5 | 下载完自动转 MP4 | M4 | ts/mkv/webm/m3u8 完成后产出 `.mp4` 且可播放 |
| R6 | 去水印最好 | M6 | 自素材 delogo 前后对比；默认关闭 |
| R7 | 不要用户设置，装好即用 | M0 | 全新机器零配置完成一次下载与一次扩展接管 |
| R8 | 设置里有下载工具常用设置 | M8 | 设置页覆盖目录/并发/限速/接管/后处理/代理等 |
| R9 | 打包 exe，重装系统直接装 | M8 | 未装依赖的干净 VM 装完能下载 |
| R10 | 开源到 GitHub | M0 起 | LICENSE/copyright/合规说明齐备 |


---

## 2. 总体架构（进程与边界）

```
┌──────────────────────── Chrome / Edge ────────────────────────┐
│ Sun_Tey 扩展 (MV3)                                             │
│   L1 接管层  downloads.onDeterminingFilename + 类型表 + 修饰键   │
│   L2 嗅探层  performance 资源扫描 / DOM 扫描 / MAIN-world hook   │
│   L3 呈现层  <video> 悬浮按钮 + 页面媒体面板                     │
└───────────────┬───────────────────────────────────────────────┘
                │ loopback HTTP + 首启自动握手 token（零配置）
┌───────────────▼───────────── Sun_Tey.exe (UI 壳) ─────────────┐
│  任务列表 / 媒体面板 / 设置页 / 托盘 / 通知                       │
└───────────────┬───────────────────────────────────────────────┘
                │ 本地 IPC（命名管道 / loopback，仅 127.0.0.1）
┌───────────────▼──────────── suntey-core.exe (内核) ───────────┐
│  Dispatcher（唯一入口）                                         │
│   ├─ httpx   多线程 Range + 慢启动扩连 + 慢连接偷块 + 看门狗      │
│   ├─ bttx    libtorrent：种子/磁力/选文件/做种/trackers 合并      │
│   ├─ hlsx    HLS/DASH 清单 + 分片并发 + AES-128 + fMP4 重组      │
│   ├─ store   任务/分片/配置持久化（重启自动恢复）                  │
│   ├─ post    完成后处理管线：转码→去水印→校验→移动→通知→脚本        │
│   └─ serve   REST API（/api/v1/*）+ 扩展握手                     │
└───────────────┬───────────────────────────────────────────────┘
                └─ 外部进程 ffmpeg.exe（转封装/转码/delogo）
```

**边界规则（不可违反）**

1. UI 壳不直接碰网络与磁盘分片，一切经 Dispatcher。
2. 内核不弹模态框、不抢焦点；需要用户决策时返回结构化状态由 UI 呈现。
3. 任何模块不得自建 HTTP 客户端绕过 `httpx` 的连接治理（诊断命令除外，须标注）。
4. 扩展不得向任何远端发送页面内容；发给本地的字段走白名单（url/referer/UA/必要 cookie）。
5. 端口默认随机 + 回环监听；不暴露局域网。

## 3. 仓库目录结构（施工后必须长成这样）

```
Sun_Tey/
  README.md                项目说明（含「能下/不能下」诚实清单）
  LICENSE                  GPL-3.0-only（提案，见规则 §6）
  docs/
    施工图-BLUEPRINT.md     本文件
    规则要求-RULES.md       工程规则
    知识库索引.md            研究知识库入口（01–05 + 官方原文获取方式）
    站点规则/               嗅探规则说明（不含受版权素材）
    验收报告/               每个里程碑的实测记录（M0..M8）
  core/                    Go 内核（module: github.com/tey-sun/suntey/core）
    cmd/suntey-core/       main
    internal/dispatcher/   任务生命周期唯一入口
    internal/download/httpx/   R1
    internal/download/bttx/    R2 R3
    internal/download/hlsx/    R4 R5
    internal/post/             R5 R6（新模块，M4）
    internal/store/            持久化
    internal/server/           REST + 扩展握手 + 零配置
    internal/config/           默认值即「开箱可用」
  app/                     UI 壳（沿用前代实现的技术栈）
    index.html  src/  dist/
  extension/               MV3 扩展
    manifest.json  background.js  content.js  inject.js  panel/  _locales/
  packaging/               NSIS 脚本 + 图标 + 版本信息 + binaries 清单
  scripts/                 构建/测试/冒烟/发布脚本（幂等）
  tests/                   跨模块集成测试与测试数据生成器
  .github/workflows/       CI：vet / test / build / package
```

**禁止**：在仓库根目录堆源码；把 `binaries/ffmpeg.exe`（约 170 MB）提交进 Git（走 Git LFS 或发布期下载，见规则 §7）。

---

## 4. 里程碑

> 纪律：**一次只做一个里程碑**；每个里程碑结束必须留下 `docs/验收报告/Mn.md`（命令 + 真实输出摘要 + 未验证项）。
> 未跑过的判据一律写 `[U]`，禁止写「应该没问题」。

### M0 — 干净落地与绿色基线（阻塞一切）

**目标**：把已验证资产迁入新仓，并让 `vet/test/build` 全绿。当前前代实现带一个失败测试，**不能原样搬**。

输入事实：`docs/知识库/04-前代实现体检.md` —— `go test ./...` 唯一失败 `bttx TestFileSelectionSwitch`
（`bttx_test.go:575`：第二次 `SetFileSelection` 得到 `ErrBTNotRunning`，`bttx.go:214`）`[V]`

施工：
1. 迁入内核与 UI、扩展、打包脚本；目录改成 §3 结构；`go.mod` module 路径改 `github.com/tey-sun/suntey/core`。
2. 全量标识替换：`fastdl`→`suntey`、`FastDL`→`Sun_Tey`（含注册表键、命名管道、User-Agent、NSIS 产品名、扩展 ID 相关字段）。
3. 删除 IDE 垃圾文件与根目录临时文件（`# Tools Configuration.md`、`_smoke13.txt` 等）`[V 已记录]`。
4. **修 P1-1**：先加日志确认 live 注册表的注册/注销时机，再改；不允许「改到测试变绿」。
   要求补两条测试：(a) 运行中连续两次改选文件均生效；(b) 任务暂停后改选→恢复后选择仍生效。
5. 建 CI：`go vet ./...`、`go test ./... -count=1`、`go build ./...`。

验收：
```
cd core && go vet ./... && go test ./... -count=1 && go build ./...
```
判据：三条命令 EXIT=0；`bttx` 全绿且含上述两条新测试；`docs/验收报告/M0.md` 落盘。

禁止：搬 gopeed/aria2 源码（GPL 复制风险）；搬 IDM/迅雷 任何二进制、资源、协议实现。

---

### M1 — IDM 级多线程（R1，差异化核心）

**目标**：把「静态均分 N 路」升级为「慢启动扩连 + 慢连接偷块 + 动态分片」。

输入事实（`docs/知识库/01-gopeed-内核逆向.md`，均为 `[S]`，按行为重新实现，**不得抄源码**）：
- 慢启动扩连 `slowStartController`：批大小 `1→2→4→16→256`（平方增长，`from==1` 特判为 2，乘法前判溢出并 cap 到 `maxConnections`）；每条连接拿到响应即 `onConnectSuccess`；连接失败也要递减 pending，避免卡死；429/限流时 `paused=true` 暂停扩张。
- 慢连接偷块：`stealThresholdSeconds=3`（受害者剩余 >3s 才偷）、`stealMinChunkSize=512KB`（避免碎块）。
- 错误分类：区分「可重试 Range 错误」与「必须整体回退」`errRangeRequestIgnored / errInvalidRangeResponse / errIncompleteResponse`。
- 借鉴 aria2：`-k/--min-split-size`（小于 2×SIZE 不切分）、`-x`（单服务器上限）与 `-s`（总连接数）是**两个独立旋钮**`[S:official/aria2-help.txt]`。

施工（`internal/download/httpx/`）：
1. 新增 `minSplitSize`（默认 4 MiB）+ `maxConnPerHost`（默认 8）+ `maxConns`（默认 16）三旋钮，小文件不再被切碎。
2. 实现 `slowStart` 批次扩张，接进现有 `runRanges`。
3. 实现 `steal` 再平衡：从进度最慢连接取「未开始」的尾部块；被偷块 ≥512 KB，受害者剩余预计 >3 s。
4. 429/`Retry-After`/`403` 分类处理；服务器不支持 Range 时**整文件单连接回退**并落一次日志。
5. 保留现有 `idleGuard` / `watchdog` / chunk 级快照续传。

验收（真实网络 + 本地服务器各一组）：
```
go test ./internal/download/httpx/ -run 'Dynamic|Steal|SlowStart' -count=1
python scripts/bench_multithread.py --url <测试文件> --conns 1,8,16 --repeat 3
```
判据：本地服务器下 8/16 路相对 1 路有可复现加速（记录 MB/s 表）；单连接回退路径有测试覆盖；`docs/验收报告/M1.md` 记录数据表。

---

### M2 — 磁力的可解释性 + 选文件（R3，迅雷体感）

**目标**：磁力不再「静默低进度」，并在下载前给出文件清单勾选。

输入事实：`docs/知识库/05-参考工具对照.md`（迅雷三态语义）、`docs/知识库/01` §2（trackers 合并、DHT、peer relevance）`[S]`

施工（`internal/download/bttx/`）：
1. 磁力状态机三态 + 兜底：`元数据加载中` / `连接 N 个对等点（tracker|dht）` / `暂无可用资源`（>N 秒零 peer 时给出可操作建议）。
2. 元数据到达后进入「待选文件」态，UI 出清单（大小/路径/首尾预览），默认全选，勾选后开始下。
3. tracker 来源：内置公共订阅合并去重 + 用户自定义；scheme 白名单 `udp/http/https/ws/wss`，拒绝带 userinfo 的 URL；记录每个 tracker 来源以便解释「速度从哪来」。
4. 修完 M0 的选文件回归后，把「运行中改选」与「待选文件」两条路径统一到同一个 selection 模型。

验收：
```
go test ./internal/download/bttx/ -count=1
python scripts/live_smoke_magnet.py --magnet <公开磁力> --timeout 120
```
判据：热门公开磁力 60 s 内出元数据并出现非零速度；拔网→恢复不丢进度；三态提示在 UI 可见；`docs/验收报告/M2.md`。

---

### M3 — 浏览器扩展：接管 + 视频嗅探（R4，用户第一优先级）

**目标**：装好扩展即接管下载；页面里的非 DRM 视频出现下载按钮/面板。

输入事实：`docs/知识库/03-浏览器扩展与视频嗅探.md`
- gopeed 官方扩展**不做嗅探**，只接管 `[S]` → 嗅探是 Sun_Tey 的差异点。
- IDM 扩展行为：按扩展名列表接管、修饰键强制/豁免、找不到视频时静默 `[S/B]`。
- 能力边界（必须写进 UI 与 README）：直链 mp4/webm/ts ✅、HLS 无 DRM ✅、fMP4 init+segments ⚠️可实现、MSE/blob ⚠️只能抓背后的分片、**DRM ❌ 明确不做**。

分层施工（`extension/`）：
| 层 | 做法 | 关键文件 |
|---|---|---|
| L1 接管 | `downloads.onDeterminingFilename` + 类型白名单 + Alt/Shift 覆盖 | `background.js` |
| L2 嗅探 | `performance.getEntriesByType('resource')` 扫媒体后缀与类型；`MutationObserver` 监听 `<video>/<source>` | `content.js` |
| L2 增强 | MAIN world hook `fetch/XHR/HTMLMediaElement.src/URL.createObjectURL/MediaSource`，记录 MSE 背后的 init+segment 序列 | `inject.js` |
| L3 呈现 | `<video>` 悬浮按钮 + 页面媒体面板（多码率/多语言合并） | `panel/*` |
| L4 传输 | loopback HTTP + 首启免 token 自动握手（**零配置**） | `background.js` |
| L5 拒绝 | 识别 license 请求 / `keySystem` → 明确提示「加密流，无法下载」 | `content.js` |

验收：
```
python scripts/ext_e2e.py          # 加载扩展的端到端脚本
```
判据：① 普通文件下载被接管并出现在任务列表；② 三个不同类型站点（直链 / m3u8 / fMP4）能列出候选并成功下载；③ DRM 站点给出明确拒绝文案，**不出现假进度条**；④ 未找到视频时完全静默；⑤ `docs/验收报告/M3.md`。

---

### M4 — 完成后处理管线（R5，前代实现最大缺口 P1-2）

**目标**：任何格式下完自动变 MP4，并可挂后续动作。这是 gopeed 有、前代实现没有的东西。

输入事实：`docs/知识库/02-gopeed-官方契约.md` §4（gopeed 的 webhook + 完成后执行脚本）`[S]`；
`docs/知识库/04` P1-2「非 HLS 的 ts/mkv/flv/webm 不会自动转 MP4，无钩子抽象」`[S]`；
`docs/知识库/01` §3（gopeed 把 ffmpeg 做成注入运行时；Sun_Tey 取前代实现的直连 `findFFmpeg()+remuxToMP4()` 路线）`[S]`

新模块 `internal/post/`：

```go
type Hook interface {
    Name() string
    Match(t *model.Task) bool
    Apply(ctx context.Context, t *model.Task) (Artifact, error)
}
```

内置顺序（默认开启前 4 步，全链路失败**不得**让任务变红）：

| # | Hook | 行为 | 默认 |
|---|---|---|---|
| 1 | `probe` | ffprobe 判定容器/编码/是否已是 MP4 | 开 |
| 2 | `remux2mp4` | 兼容编码只转封装（`-c copy -movflags +faststart`） | 开 |
| 3 | `transcode2mp4` | H.264/AAC 重编码（硬件加速优先，回退软编） | 开 |
| 4 | `cleanup` | 成功后删中间文件（失败保留原文件） | 开 |
| 5 | `delogo` | 固定矩形台标修复（**默认关闭**，见 M6） | 关 |
| 6 | `subtitles` | 内封字幕（mkv → mp4 mov_text） | 开 |
| 7 | `move` | 移动到用户目录 | 关 |
| 8 | `notify` | 系统通知（完成/失败） | 开 |
| 9 | `script` | 外部脚本/webhook，环境变量传任务信息（等价 gopeed 能力） | 关 |
| 10 | `extract` | zip/7z/rar 解压（可选） | 关 |

施工要点：
1. 管线状态入库（`post_status`：`pending/running/done/failed/skipped` + 原因），UI 显示「正在转码」而不是「已完成」。
2. ffmpeg 缺失时的零配置策略：随包携带 → 再退回系统 PATH → 都缺则标记 `skipped(ffmpeg-missing)`，**下载结果仍算成功**。
3. 幂等：同一任务重复触发不产生二次转码（以输出文件存在 + 源哈希为键）。
4. 并发上限独立于下载并发（转码吃 CPU）。

验收：
```
python scripts/test_post_pipeline.py    # 生成 ts/mkv/webm/flv/m3u8 样本走完整链路
```
判据：5 类输入全部产出可播放 `.mp4`（`ffprobe` 校验流信息）；转码失败时原始文件保留且任务标「已下载，转码失败」；`docs/验收报告/M4.md` 附每个样本的 ffprobe 摘要。

---

### M5 — 零配置与 UI 重做（R7，用户明确不满区）

**目标**：装完不点任何设置就能用；扩展自动连上；不再出现「装不上/连不上」。

输入事实：`docs/知识库/02` §2（gopeed 扩展靠 git 安装、国内访问受阻 = 装不上的头号原因）`[S]`；
`docs/知识库/04` §4（前代实现已有随机端口 + 免 token handshake + 扩展自动填 token，方向对但**装机未验证** `[U]`）

施工：
1. 首启自检：下载目录自动创建（默认 `D:\下载` 不存在则建；无 D 盘退回 `C:\Users\<u>\Downloads`）。
2. 浏览器扩展**随安装包一起分发**，并提供「一键装到 Chrome/Edge」引导（解压到固定目录 + 打开扩展管理页 + 复制路径）。
3. 握手：UI 壳、内核、扩展三方在首启 5 s 内自动建立信任，任何一步失败在托盘气泡给**人话**原因与修复按钮。
4. 协议接管：注册 `suntey://`（`HKCU\Software\Classes`，不写 HKLM，免管理员）+ `.torrent` 文件关联（用户点一下即可，不默认抢）。
5. 任务卡片默认信息：进度/速度/剩余时间/线程数/来源；失败原因用人话（「服务器拒绝」而非 `HTTP 403`）。
6. 磁力三态（M2）在 UI 呈现。

验收（必须真机走一遍，记录 `[U]→[V]`）：
```
scripts/install_fresh_vm.ps1   # 干净 VM：装 → 开 → 拖一个链接 → 完成
```
判据：全新系统安装后**不进入设置页**即可完成 ①HTTP 直链 ②磁力 ③扩展接管下载 三项；`docs/验收报告/M5.md` 含每步截图与耗时。

---

### M6 — 去水印（R6，限定用途，默认关闭）

**范围（硬边界，写进 README 与代码注释）**：仅对**用户自己有权处理的素材**做**固定位置**台标/角标矩形修复。
不做：DRM 绕过、平台水印去除、批量站点搬运。

施工：
1. UI：在视频上框选矩形 → 生成 `delogo=x:y:w:h` 参数；参数随任务保存。
2. 内核 `post` 第 5 步执行；默认关闭，勾选后仅对当前任务或「同类」任务生效（不全局默认开）。
3. 处理结果与原件并存（`原名_去水印.mp4`），避免破坏原始素材。

验收：自录素材前后对比图入 `docs/验收报告/M6.md`；README 含「用途与法律边界」段。

---

### M7 — 反爬/受限站点策略（诚实边界）

**目标**：把「下不动」变成「可解释 + 可自救」，而不是失败黑盒。

| 场景 | 策略 |
|---|---|
| 403 需 Referer/Cookie | 扩展把 url/referer/UA/必要 cookie 随任务下发（白名单字段） |
| 需登录/会员 | 明确提示「需要登录」，提供「从浏览器复制为带凭证下载」入口 |
| 分片过期（m3u8 token 短时） | 边下边刷新清单；失败分片重试前重取清单 |
| 站点专用规则 | `docs/站点规则/` 以**配置**表达（域名 + 正则 + 头模板），不写第三方专有协议 |
| DRM | 拒绝并说明；不实现解密 |

验收：三类场景各有可复现用例（可用本地模拟服务器构造 403/过期 token）。

---

### M8 — 常用设置 + 安装包（R8 R9）

**设置页（一页，分 5 组，全部有合理默认值）**
1. 下载：默认目录、并发任务数、单任务线程数、最小分片、限速（全局/定时）
2. 浏览器：接管开关、接管文件类型列表、修饰键豁免、扩展安装引导
3. 视频：自动转 MP4、编码器/画质档位、硬件加速、字幕、去水印
4. BT/磁力：监听端口、加密、做种时长/比率、tracker 订阅、连接上限
5. 高级：代理（HTTP/SOCKS5）、磁盘预分配、完成后动作、日志级别、API Token、开机自启、主题/语言

**安装包（`packaging/`）**
- NSIS 单文件安装包：内核 + UI + 扩展 zip + ffmpeg.exe + 卸载器
- 版本信息与图标；升级覆盖安装保留用户数据（数据目录与安装目录分离）
- 数据目录：`%LOCALAPPDATA%\Sun_Tey\`（任务库、日志、配置）；卸载时询问是否删除
- ffmpeg 版本锁：`packaging/binaries.lock.json` 记录 name/version/url/sha256，构建脚本校验后打入
- 产出 `Sun_Tey-Setup-<ver>.exe`，CI 上构建，发布为 GitHub Release

验收：
1. 干净 Windows（无 Go/Node/ffmpeg）安装 → 直接下载 → 转码 → 完成；卸载 → 无残留（或仅保留用户数据，需说明）。
2. 重启系统后任务自动恢复。
3. `docs/验收报告/M8.md` 记录安装包大小、安装耗时、卸载结果。

---

## 5. 内核 REST 契约（仅回环；与 gopeed 生态形状兼容）

沿用 gopeed 的路径形状便于生态工具对接 `docs/知识库/02` §1 `[S]`，但**响应信封用自己的** `{ok, data, error}`。

```
POST   /api/v1/task                 创建（url / magnet / torrent / savePath / select）
GET    /api/v1/tasks                列表（分页、状态过滤）
GET    /api/v1/task/{id}            详情
PATCH  /api/v1/task/{id}            改（名称/保存路径/选文件/限速）
DELETE /api/v1/task/{id}            删除（可选删文件）
POST   /api/v1/task/{id}/start|pause|retry
GET    /api/v1/task/{id}/stats      速度/进度/线程数/peer 数/后处理状态
GET    /api/v1/config  PUT  /api/v1/config
GET    /api/v1/extension/handshake  扩展零配置自动配对
GET    /api/v1/events               SSE：任务与后处理状态推送
```

约束：所有写操作需 `X-SunTey-Token`（首启由握手自动下发）；仅绑定 `127.0.0.1`；错误体必须含 `reason`（机器码）与 `hint`（人话）。

## 6. 数据模型（关键字段，勿随意改名）

```
Task: id, kind(http|torrent|magnet|playlist), name, url, savePath, status,
      size, done, speed, connCount, peers, trackedFrom,
      files[]: {index, path, size, selected, done},
      post: {status, stage, reason, output[], startedAt, finishedAt},
      error: {reason, hint}, createdAt, updatedAt
```

规则：状态枚举一经发布只增不改；`status` 与 `post.status` 分离（下载成功 ≠ 后处理成功）。

## 7. 测试策略（门禁不可放宽）

| 层 | 内容 | 何时跑 |
|---|---|---|
| 单元 | `go test ./...`（含 httpx 动态分片、bttx 选文件、post 幂等） | 每次提交 |
| 契约 | REST 路由 + 鉴权 + 错误体形状 | 每次提交 |
| 集成 | 本地 HTTP 服务器 + 合成种子 + 合成 m3u8 | 每次提交 |
| 端到端 | 扩展加载 + 真实站点三类 + 完成后转码 | 每个涉及扩展/后处理的里程碑 |
| 装机 | 干净 VM 安装→下载→卸载 | M8 |

分层要求：`go test -short` 必须在 **90 秒**内跑完（前代实现 httpx 单包 49 s `[V]`，长用例打 `long` 标签，只在里程碑末跑）。

## 8. 已知风险与对策

| 风险 | 影响 | 对策 |
|---|---|---|
| 「下载任何视频」被理解为包含 DRM | 法律/口碑 | README + UI 明示「任何**非 DRM** 视频」；DRM 明确拒绝 |
| MSE/blob 无法按 URL 下 | 用户以为坏了 | 走分片重组；不可下时禁用按钮并说明原因 |
| ffmpeg 体积（约 170 MB `[B]`） | 仓库膨胀、安装慢 | Git LFS 或发布期下载；`binaries.lock.json` 锁 sha256 |
| 磁力 NAT 内成功率无长期数据 `[U]` | 「迅雷式」承诺打折 | 打点：元数据耗时/peer 来源分布；UI 三态；不承诺「加速」 |
| 动态多线程改动破坏续传兼容 | 用户旧任务坏 | chunk 快照带版本号 + 迁移测试 |
| 扩展全量 host 权限 + 读 cookie | 隐私质疑 | 字段白名单写进 README；提供「仅手动添加」模式 |
| 内核网络监听被安全软件拦 | 首启失败 | 仅回环 + 随机端口；失败给人话与一键修复 |

## 9. 完成的定义（DoD，逐条打勾）

一个里程碑只有全部满足才算完成：需求可追溯到 R#；代码已改（无占位逻辑）；`vet/short test` 绿；集成或端到端至少一项**真跑过**；`docs/验收报告/Mn.md` 有命令与真实输出；新增依赖有许可证记录；未验证项显式标 `[U]`。

## 10. 顺序与并行建议

```
M0（阻塞）→ M1 ─┐
                ├→ M4 → M5 → M8
        M2 ─────┘              ↘
        M3 ──────────────────→  发布
        M6/M7 可在 M4 之后并行插入
```

M1 与 M2 可并行（写集不相交：`httpx/` 与 `bttx/`）；M3（extension/）与 M4（internal/post/）可并行。