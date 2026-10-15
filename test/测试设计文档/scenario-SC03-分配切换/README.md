# SC03 分配切换（已删除）

> 2026-10-15：本场景的全部测试用例已删除。

原"主备角色切换"场景验证 primary/standby 角色生命周期（降备拒绝服务、提升恢复、摘除）。
2026-10-14 单实例化改造（见 `docs/zh_cn/modifications/2026-10-14-epp-single-instance/`）已删除
`cell.Role` 与角色概念，原场景失去验证对象；分配命中/未命中语义由
`pkg/poller/epp_data_test.go`（`TestEppDataHandleLifecycle` /
`TestResolveAssignmentEntry`）与 SC15-TC04 覆盖。

目录保留，内容已清空。
