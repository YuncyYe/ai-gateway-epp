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

// Package sc09: 原"双实例互备组"场景（组内主备分工与 FailoverFlip）。
//
// 2026-10-15 起本场景的全部测试用例已删除：单实例化改造
// （docs/zh_cn/modifications/2026-10-14-epp-single-instance/）后每组恰 1
// 实例、无 standby 路径，原场景失去验证对象。组分配语义由
// pkg/poller/epp_data_test.go 覆盖。目录保留。
package sc09
