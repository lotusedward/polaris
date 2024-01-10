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
	apimodel "github.com/polarismesh/specification/source/go/api/v1/model"
	apiservice "github.com/polarismesh/specification/source/go/api/v1/service_manage"
	apitraffic "github.com/polarismesh/specification/source/go/api/v1/traffic_manage"

	api "github.com/polarismesh/polaris/common/api/v1"
	"github.com/polarismesh/polaris/common/model"
	"github.com/polarismesh/polaris/common/utils"
)

// CreateRateLimits creates rate limits for a namespace.
func (svr *ServerAuthAbility) CreateRateLimits(
	ctx context.Context, reqs []*apitraffic.Rule) *apiservice.BatchWriteResponse {
	authCtx := svr.collectRateLimitAuthContext(ctx, reqs, model.Create, "CreateRateLimits")

	_, err := svr.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx)
	if err != nil {
		return api.NewBatchWriteResponseWithMsg(apimodel.Code_NotAllowedAccess, err.Error())
	}

	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)

	return svr.targetServer.CreateRateLimits(ctx, reqs)
}

// DeleteRateLimits deletes rate limits for a namespace.
func (svr *ServerAuthAbility) DeleteRateLimits(
	ctx context.Context, reqs []*apitraffic.Rule) *apiservice.BatchWriteResponse {
	authCtx := svr.collectRateLimitAuthContext(ctx, reqs, model.Delete, "DeleteRateLimits")

	_, err := svr.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx)
	if err != nil {
		return api.NewBatchWriteResponseWithMsg(apimodel.Code_NotAllowedAccess, err.Error())
	}

	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)

	return svr.targetServer.DeleteRateLimits(ctx, reqs)
}

// UpdateRateLimits updates rate limits for a namespace.
func (svr *ServerAuthAbility) UpdateRateLimits(
	ctx context.Context, reqs []*apitraffic.Rule) *apiservice.BatchWriteResponse {
	authCtx := svr.collectRateLimitAuthContext(ctx, reqs, model.Modify, "UpdateRateLimits")

	_, err := svr.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx)
	if err != nil {
		return api.NewBatchWriteResponseWithMsg(apimodel.Code_NotAllowedAccess, err.Error())
	}

	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)

	return svr.targetServer.UpdateRateLimits(ctx, reqs)
}

// EnableRateLimits 启用限流规则
func (svr *ServerAuthAbility) EnableRateLimits(
	ctx context.Context, reqs []*apitraffic.Rule) *apiservice.BatchWriteResponse {
	authCtx := svr.collectRateLimitAuthContext(ctx, nil, model.Read, "EnableRateLimits")

	_, err := svr.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx)
	if err != nil {
		return api.NewBatchWriteResponseWithMsg(apimodel.Code_NotAllowedAccess, err.Error())
	}

	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)

	return svr.targetServer.EnableRateLimits(ctx, reqs)
}

// GetRateLimits gets rate limits for a namespace.
func (svr *ServerAuthAbility) GetRateLimits(
	ctx context.Context, query map[string]string) *apiservice.BatchQueryResponse {
	authCtx := svr.collectRateLimitAuthContext(ctx, nil, model.Read, "GetRateLimits")

	_, err := svr.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx)
	if err != nil {
		return api.NewBatchQueryResponseWithMsg(apimodel.Code_NotAllowedAccess, err.Error())
	}

	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)

	return svr.targetServer.GetRateLimits(ctx, query)
}

// ExportRateLimits export rate limits
func (svr *ServerAuthAbility) ExportRateLimits(
	ctx context.Context, query map[string]string) *apiservice.BatchQueryResponse {
	authCtx := svr.collectRateLimitAuthContext(ctx, nil, model.Read, "ExportRateLimits")

	_, err := svr.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx)
	if err != nil {
		return api.NewBatchQueryResponseWithMsg(apimodel.Code_NotAllowedAccess, err.Error())
	}

	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)

	return svr.targetServer.ExportRateLimits(ctx, query)
}

// ImportRateLimits Import rate limits
func (svr *ServerAuthAbility) ImportRateLimits(
	ctx context.Context, configFiles []*apiconfig.ConfigFile) *apiservice.BatchWriteResponse {
	authCtx := svr.collectRateLimitAuthContext(ctx, nil, model.Read, "ImportRateLimits")

	_, err := svr.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx)
	if err != nil {
		return api.NewBatchWriteResponseWithMsg(apimodel.Code_NotAllowedAccess, err.Error())
	}

	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)

	return svr.targetServer.ImportRateLimits(ctx, configFiles)
}
