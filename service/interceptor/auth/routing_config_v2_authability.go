/**
 * Tencent is pleased to support the open source community by making Polaris available.
 *
 * Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package service_auth

import (
	"context"

	apiconfig "github.com/polarismesh/specification/source/go/api/v1/config_manage"
	apiservice "github.com/polarismesh/specification/source/go/api/v1/service_manage"
	apitraffic "github.com/polarismesh/specification/source/go/api/v1/traffic_manage"

	api "github.com/polarismesh/polaris/common/api/v1"
	"github.com/polarismesh/polaris/common/model"
	"github.com/polarismesh/polaris/common/utils"
)

// CreateRoutingConfigsV2 批量创建路由配置
func (svr *ServerAuthAbility) CreateRoutingConfigsV2(ctx context.Context,
	req []*apitraffic.RouteRule) *apiservice.BatchWriteResponse {

	// TODO not support RouteRuleV2 resource auth, so we set op is read
	authCtx := svr.collectRouteRuleV2AuthContext(ctx, req, model.Read, "CreateRoutingConfigsV2")
	if _, err := svr.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx); err != nil {
		return api.NewBatchWriteResponse(convertToErrCode(err))
	}
	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)
	return svr.targetServer.CreateRoutingConfigsV2(ctx, req)
}

// DeleteRoutingConfigsV2 批量删除路由配置
func (svr *ServerAuthAbility) DeleteRoutingConfigsV2(ctx context.Context,
	req []*apitraffic.RouteRule) *apiservice.BatchWriteResponse {

	authCtx := svr.collectRouteRuleV2AuthContext(ctx, req, model.Read, "DeleteRoutingConfigsV2")
	if _, err := svr.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx); err != nil {
		return api.NewBatchWriteResponse(convertToErrCode(err))
	}
	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)
	return svr.targetServer.DeleteRoutingConfigsV2(ctx, req)
}

// UpdateRoutingConfigsV2 批量更新路由配置
func (svr *ServerAuthAbility) UpdateRoutingConfigsV2(ctx context.Context,
	req []*apitraffic.RouteRule) *apiservice.BatchWriteResponse {

	authCtx := svr.collectRouteRuleV2AuthContext(ctx, req, model.Read, "UpdateRoutingConfigsV2")
	if _, err := svr.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx); err != nil {
		return api.NewBatchWriteResponse(convertToErrCode(err))
	}
	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)
	return svr.targetServer.UpdateRoutingConfigsV2(ctx, req)
}

// EnableRoutings batch enable routing rules
func (svr *ServerAuthAbility) EnableRoutings(ctx context.Context,
	req []*apitraffic.RouteRule) *apiservice.BatchWriteResponse {

	authCtx := svr.collectRouteRuleV2AuthContext(ctx, req, model.Read, "EnableRoutings")
	if _, err := svr.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx); err != nil {
		return api.NewBatchWriteResponse(convertToErrCode(err))
	}
	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)
	return svr.targetServer.EnableRoutings(ctx, req)
}

// QueryRoutingConfigsV2 提供给OSS的查询路由配置的接口
func (svr *ServerAuthAbility) QueryRoutingConfigsV2(ctx context.Context,
	query map[string]string) *apiservice.BatchQueryResponse {

	return svr.targetServer.QueryRoutingConfigsV2(ctx, query)
}

func (svr *ServerAuthAbility) ExportRoutings(ctx context.Context,
	query map[string]string) *apiservice.BatchQueryResponse {
	authCtx := svr.collectRouteRuleV2AuthContext(ctx, nil, model.Read, "ExportRoutings")
	if _, err := svr.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx); err != nil {
		return api.NewBatchQueryResponse(convertToErrCode(err))
	}
	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)
	return svr.targetServer.ExportRoutings(ctx, query)
}

func (svr *ServerAuthAbility) ImportRoutings(ctx context.Context,
	configFiles []*apiconfig.ConfigFile) *apiservice.BatchWriteResponse {
	authCtx := svr.collectCircuitBreakerRuleV2AuthContext(ctx, nil, model.Read, "ImportRoutings")
	_, err := svr.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx)
	if err != nil {
		return api.NewBatchWriteResponse(convertToErrCode(err))
	}

	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)
	return svr.targetServer.ImportRoutings(ctx, configFiles)
}
