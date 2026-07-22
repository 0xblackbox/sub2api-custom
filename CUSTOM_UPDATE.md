# SubtoProxy 安全更新通道

这个分支在官方 Sub2API 的基础上保留 `POST /v1/responses` 的
`background=true, store=true` 原生上游后台执行兼容层，并增加 Responses
重型 Web Search 的独立排队、取消、短 GET 轮询恢复和脱敏可观测性。
网页更新功能固定到：

`0xblackbox/sub2api-custom`

## 工作方式

1. GitHub Actions 每 6 小时检查一次官方最新稳定 Release。
2. 在干净的官方源码上应用 `custom/patches/subtoproxy.patch`。
3. 构建前端并运行后端兼容性测试。
4. 只有补丁可完整应用且测试全部通过，才发布自定义 Release。
5. 服务器网页只检查和下载这个自定义 Release；不会再下载官方原版二进制。
6. 管理后台点击一次“更新”后，校验 SHA-256、原子替换二进制并自动重启。

如果官方改动与兼容层冲突，流水线会直接失败，不会发布不安全的更新，网页也不会出现
新版本提示。修复补丁并重新运行流水线后，更新才会重新可用。

## 手动触发检查

在 GitHub 仓库的 **Actions → SubtoProxy safe release → Run workflow** 点击一次即可。
这一步不会操作生产服务器，只负责在检查通过后产生可供网页一键安装的 Release。

## 安全边界

- 仓库不保存服务器 IP、SSH 私钥、管理员密码或 API Key。
- Release 只包含 Linux amd64 二进制与 `checksums.txt`。
- 应用内更新器仅接受 GitHub 官方下载域名，并在替换前校验 SHA-256。
- 每次替换会保留 `.backup`，服务器上的完整部署备份仍独立保留。
