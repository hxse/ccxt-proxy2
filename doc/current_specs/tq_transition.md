# TQ 换月过渡价格

## 公开接口

过渡附在 `/tq/fetch_underlying_symbol` 的真实换月节点上。只有传 transition_timeframe 才启用；无周期时省略 transition 字段。有周期但缺少可靠旧价格时返回 []。

```text
GET /tq/fetch_underlying_symbol?symbol=KQ.m@SHFE.rb&start_time=1790125200000000000&end_time=1790211600000000000&transition_timeframe=5m&transition_bars=10
```

周期为 s/m/h/d/w 正整数，换算后符合 TQ duration_seconds 规则；不把 M 当分钟或固定月。transition_bars 默认 10，范围为 1..SDK_WINDOW−1，当前上限 9999。仅传数量不启用过渡，传周期需要历史范围；非法周期/数量/缺范围分别为 400 TQ_INVALID_TRANSITION_TIMEFRAME / TQ_INVALID_TRANSITION_BARS / TQ_INVALID_DATE_RANGE。

```json
{"date":"2026-09-24","symbol":"KQ.m@SHFE.rb","underlying_symbol":"SHFE.rb2701","old_symbol":"SHFE.rb2610","transition":[{"datetime":1790211600000000000,"old_open":3500,"old_high":3510,"old_low":3490,"old_close":3505,"old_volume":100}]}
```

示例只说明格式。old_ 前缀表示旧合约真实原始价格，不复权，不改动主连或加权行情。一个窗口可跨自然日，不按天重新累计。

## 起点与单次证据

先完成[元数据核验](tq_metadata.md)，只处理已生效节点；未来预公告不触发过渡。完整身份包含主连 symbol、真实 roll_date、新/旧合约、秒制周期和 default 原始价格 variant，N 不属于身份。

缓存已有完整 K≥N 时读前 N 根，不请求旧价格或缩短缓存。否则直接使用无落盘 SDK 获取入口，单次请求旧合约该周期最新 SDK 窗口（目前一万根），只取稳定副本，不将一万根写入普通行情表。

必须找到 roll_date 当天第一根，且同次序列有其前驱、前驱属于更早交易日、id 相邻。夜盘/周末结合既有日历处理；不按 interval 推算连续。窗口从当天中途开始、节点早于窗口、不能证明起点或大周期不能唯一定位时返回 []，不试探更多窗口，不为上市首日补历史。

目标前 N＋1 根必须来自同一次 SDK 副本，id 连续、时间严格升序、有限 OHLCV、价格关系合法、volume 非负。不能与已有 K 或另一请求拼凑。非法目标/证明行为 422 TQ_INVALID_TRANSITION_WINDOW，网络失败保持准确错误，不伪装空成功。

## 提交与并发

含 N＋1 时，完整交 submit_transition_window(identity,N,batch)，末根标记 unknown；缓存调用与普通行情相同的尾根筛选，原子保存前 N 根，响应也只返回前 N 根。证明行不落盘，可信窗口读取不再去尾。

不足 N＋1 但起点可靠时，返回实际已有的最多 N 根，完全不写窗口，原有 K 保持；短结果也可能由到期、历史不足或源窗口限制产生，不宣称仍在走。

schema 3→4 原子新增 transition_windows/transition_rows。每个完整身份只有一个 segment_id，kind=transition、time_unit=ns；窗口表保存身份，cache_segments 保存实际首尾/K，价格表仅必要 old_ 数值，不保存整份旧合约历史。

写锁内复查 K：N<K 不替换，N=K 可整体修订，N>K 必须本次完整窗口整体替换。行与元数据同事务；失败回滚，读只见完整旧版或新版，不逐行混合补长。read_transition_prefix 返回完整前缀或 miss。

enable_cache=false 禁读写窗口，仍执行全部证据校验。没有 old_symbol 时直接空过渡。节点身份修订时使用新身份，不能沿用旧窗口。

## 等待与验证

每次 SDK 获取沿用十秒预算；整个含过渡映射操作另有 45 秒总预算，超时为 504 TQ_TRANSITION_TIMEOUT。取消后不启动下一窗口；已独立提交的完整窗口可保留，不能把整份未完成响应当成功。普通写库失败不丢已验证网络价格，但不得宣称已持久化。

离线覆盖 5→3→10、同长修订、迟到短写、单次不足、起点缺失、证明行非法、并发读取/替换、回滚、总超时及参数拒绝。明确断言原始旧合约窗口不写普通 OHLCV 表；未传周期不取旧价格。在线只留最终手动 live 只读入口，过旧窗口无法取得过渡是已接受限制。
