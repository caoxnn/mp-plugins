# 站点限速（TrackerLimit）

按 tracker 给 Transmission 里的种子上传限速 + peer 上限：定时巡检，读当前值、只在和规则不一致时改一次。

- **下载器**：用 MoviePilot 里已经配好的下载器，不另存 RPC 账号；只对 Transmission 生效，qBittorrent 实例自动跳过。
- **规则在哪改**：插件**配置页**下方的「站点清单」——一行一个站（站名用 MP 站点索引里的名字，顺序跟 MP 站点配置页一致），
  站名后面两个框填**上传限速 KB/s** / **peer 上限**，填完点页面上的保存即可（走宿主自己的配置保存）。
  留空 = 这一项不动；填 `0` = 不限速 / 不限制。同名站（多个 tracker 域名）一行一起管。
- **详情页**只做展示：站点的当前限速 / peer + 最近几轮巡检记录。
- **定时**：`巡检周期`（cron，默认 `0 */6 * * *`）；想马上跑一次就勾「立即运行一次」再保存。
- **通知**：勾「改动时发通知」后，有实际改动才会发一条。
- **外部自动化接口**（`apikey` 认证，见 `get_api()`）：`POST /api/v1/plugin/TrackerLimit/dry_run`（只报数）、
  `POST /api/v1/plugin/TrackerLimit/apply_now`（真改一次）。

匹配口径：先按「域名 → 站群别名」精确匹配（内置 86 条，比如 `tra1.m-team.cc → mteam`、`tracker.rainbowisland.co → rainbowisland`），
再退回子串匹配；同一目标的种批量下发一次限速。
