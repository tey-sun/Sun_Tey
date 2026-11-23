# 官方契约摘要（API / 扩展 / Scheme / 脚本 / Webhook）

原文在 official/live_zh_llms-full.txt（176KB，官方 LLM 导出，含每页 Source URL）与
official/swagger.json（OpenAPI）。本页只做「写代码要用的最小集」，冲突以原文为准。[S]=官方原文

## 1. REST API（对标 Sun_Tey 自己的 API 设计）

    启用：设置->高级->通讯协议 选 TCP + 端口；接口令牌自填；重启生效。
    鉴权：X-Api-Token 头 或 Authorization: Bearer（见 _re/gopeed-reverse-engineering.md:三）
    任务：POST /api/v1/task（create）、GET /task/{id}、GET /tasks、PATCH /task/{id}、
          DELETE /task/{id}、/tasks、POST /task/{id}/resolve|start|pause|continue、
          GET /task/{id}/stats、批量 /tasks/resolve|start|pause|continue|delete
    配置：GET/PUT /api/v1/config
    扩展：GET/POST/DELETE /api/v1/extension、/switch、/update、/update-check、/settings
    信封：{code, message, data}；HTTP 状态 + code 双重表达
    含义：Sun_Tey 的内核 API 已按同一形状做（dispatcher + {ok,data,error}），
      保持兼容能让 gopeed 生态工具直接对接（详见 10-BLUEPRINT.md 的 API 章节）。

## 2. JS 扩展（gopeed 的核心生态，Sun_Tey 明确不做）

    机制：manifest.json 的 scripts[] 声明 {event, match.urls, entry}；
          match 规则与 Chrome match_patterns 一致；entry 是 JS 文件，跑在 goja 里。
    运行环境：goja（纯 Go JS 解释器）。无浏览器/Node API；官方只实现 XMLHttpRequest 与 fetch。
              es6+ 大部分可用，async generator 不支持；官方脚手架用 babel 编到 es5。
    主要事件：onResolve(ctx)：读 ctx.req（url/headers），写 ctx.res（name + files[{name,req}]）
              另有 onCreate/onDone/onError/onBeforeStart（源码 pkg/download/extension.go）
    能力面：gopeed.storage.get/set、gopeed.logger.{debug,info,warn,error}、
            gopeed.createTask、gopeed.request、（桌面）gopeed.webview
    分发：靠 git 仓库去中心化管理；安装地址 = git clone URL（#子目录）；
          国内访问 github raw/git 需配 GithubMirror —— 这是 gopeed「装不上扩展」的头号原因。
    调试：扩展页连点 5 次安装按钮开开发者模式 -> 选本地目录；日志在 安装目录/logs/extension.log

## 3. Scheme 协议

    gopeed: 协议，支持创建下载任务与安装扩展（URI 格式与参数见原文「Scheme 协议」章）
    用途：浏览器/外部 App 唤起客户端；Android 可作下载接管入口。
    Sun_Tey 对应：注册 suntey:// 同类协议（Windows 写注册表 HKCU\Classes），
      作为「浏览器无法接管时的兜底」，且是单实例唤起路径。

## 4. 钩子脚本 / Webhook（Sun_Tey 直接抄能力的地方）

    Webhook：下载完成或失败推送 JSON 到配置 URL（支持多 URL）。
    脚本执行：完成后执行 .sh/.py，环境变量携带任务信息（原文「脚本执行」章给了
      move_to_hdd.sh / notify.py 两个示例）。
    结论：Sun_Tey 必须内建等价的「完成后处理」抽象（转码/去水印/移动/通知都挂在它上面），
      这是当前 前代实现 最大功能缺口（见 04-前代实现体检.md P1-2）。

## 5. SDK

    官方 @gopeed/rest（JS）与社区 gospeed_api（Python）；
    upstream/gopeed-js/packages/gopeed/index.d.ts 是扩展 API 的权威类型定义。

---

> 本文从内部研究知识库复制，已抹去本机绝对路径（原研究目录与打包产物路径以 <研究目录>、<前代产物> 代替）。
> 官方原文（gopeed 全站文档全文、OpenAPI、aria2 参数表）见上级目录说明，或 gopeed.com/zh 官网。
