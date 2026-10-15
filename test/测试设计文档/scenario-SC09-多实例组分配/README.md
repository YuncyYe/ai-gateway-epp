# SC09 多实例组分配（已删除）

> 2026-10-15：本场景的全部测试用例已删除。

原"双实例互备组"场景验证 2 实例互备组的组内主备分工与 FailoverFlip。
2026-10-14 单实例化改造（见 `docs/zh_cn/modifications/2026-10-14-epp-single-instance/`）后
每组恰 1 实例、无 standby 路径，原场景失去验证对象；组分配语义由
`pkg/poller/epp_data_test.go`（`TestEppDataHandleLifecycle` 的改派轮次）覆盖。

目录保留，内容已清空。
