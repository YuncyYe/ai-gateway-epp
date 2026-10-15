// Copyright(c) 2026 The Rainway AI Gateway (壬远AI网关) Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

// Package sc03: 原"主备角色切换"场景（assignment 角色生命周期）。
//
// 2026-10-15 起本场景的全部测试用例已删除：单实例化改造
// （docs/zh_cn/modifications/2026-10-14-epp-single-instance/）删除了
// cell.Role 与角色概念，原场景失去验证对象。分配命中/未命中语义由
// pkg/poller/epp_data_test.go 与 SC15-TC04 覆盖。目录保留。
package sc03
