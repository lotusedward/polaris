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

package config

import (
	"context"

	apiconfig "github.com/polarismesh/specification/source/go/api/v1/config_manage"

	api "github.com/polarismesh/polaris/common/api/v1"
	"github.com/polarismesh/polaris/common/model"
	"github.com/polarismesh/polaris/common/utils"
)

// CreateConfigFile 创建配置文件
func (s *serverAuthability) CreateConfigFile(ctx context.Context,
	configFile *apiconfig.ConfigFile) *apiconfig.ConfigResponse {
	authCtx := s.collectConfigFileAuthContext(
		ctx, []*apiconfig.ConfigFile{configFile}, model.Create, "CreateConfigFile")
	if _, err := s.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx); err != nil {
		return api.NewConfigFileResponseWithMessage(convertToErrCode(err), err.Error())
	}

	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)

	return s.targetServer.CreateConfigFile(ctx, configFile)
}

// GetConfigFileBaseInfo 获取配置文件，只返回基础元信息
func (s *serverAuthability) GetConfigFileBaseInfo(ctx context.Context, namespace,
	group, name string) *apiconfig.ConfigResponse {
	return s.targetServer.GetConfigFileBaseInfo(ctx, namespace, group, name)
}

// GetConfigFileRichInfo 获取单个配置文件基础信息，包含发布状态等信息
func (s *serverAuthability) GetConfigFileRichInfo(ctx context.Context, namespace,
	group, name string) *apiconfig.ConfigResponse {
	return s.targetServer.GetConfigFileRichInfo(ctx, namespace, group, name)
}

func (s *serverAuthability) QueryConfigFilesByGroup(ctx context.Context, namespace, group string,
	offset, limit uint32) *apiconfig.ConfigBatchQueryResponse {
	return s.targetServer.QueryConfigFilesByGroup(ctx, namespace, group, offset, limit)
}

// SearchConfigFile 查询配置文件
func (s *serverAuthability) SearchConfigFile(ctx context.Context, namespace, group, name,
	tags string, offset, limit uint32) *apiconfig.ConfigBatchQueryResponse {
	return s.targetServer.SearchConfigFile(ctx, namespace, group, name, tags, offset, limit)
}

// UpdateConfigFile 更新配置文件
func (s *serverAuthability) UpdateConfigFile(
	ctx context.Context, configFile *apiconfig.ConfigFile) *apiconfig.ConfigResponse {
	authCtx := s.collectConfigFileAuthContext(
		ctx, []*apiconfig.ConfigFile{configFile}, model.Modify, "UpdateConfigFile")
	if _, err := s.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx); err != nil {
		return api.NewConfigFileResponseWithMessage(convertToErrCode(err), err.Error())
	}

	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)

	return s.targetServer.UpdateConfigFile(ctx, configFile)
}

// DeleteConfigFile 删除配置文件，删除配置文件同时会通知客户端 Not_Found
func (s *serverAuthability) DeleteConfigFile(ctx context.Context, namespace, group,
	name, deleteBy string) *apiconfig.ConfigResponse {
	authCtx := s.collectConfigFileAuthContext(ctx,
		[]*apiconfig.ConfigFile{{
			Namespace: utils.NewStringValue(namespace),
			Name:      utils.NewStringValue(name),
			Group:     utils.NewStringValue(group)},
		}, model.Delete, "DeleteConfigFile")
	if _, err := s.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx); err != nil {
		return api.NewConfigFileResponseWithMessage(convertToErrCode(err), err.Error())
	}

	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)

	return s.targetServer.DeleteConfigFile(ctx, namespace, group, name, deleteBy)
}

// BatchDeleteConfigFile 批量删除配置文件
func (s *serverAuthability) BatchDeleteConfigFile(ctx context.Context, configFiles []*apiconfig.ConfigFile,
	operator string) *apiconfig.ConfigResponse {
	authCtx := s.collectConfigFileAuthContext(ctx, configFiles, model.Delete, "BatchDeleteConfigFile")
	if _, err := s.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx); err != nil {
		return api.NewConfigFileResponseWithMessage(convertToErrCode(err), err.Error())
	}

	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)

	return s.targetServer.BatchDeleteConfigFile(ctx, configFiles, operator)
}

func (s *serverAuthability) ExportConfigFile(ctx context.Context,
	configFileExport *apiconfig.ConfigFileExportRequest) *apiconfig.ConfigExportResponse {
	return s.targetServer.ExportConfigFile(ctx, configFileExport)
}

func (s *serverAuthability) ImportConfigFile(ctx context.Context,
	configFiles []*apiconfig.ConfigFile, conflictHandling string) *apiconfig.ConfigImportResponse {
	authCtx := s.collectConfigFileAuthContext(ctx, configFiles, model.Create, "ImportConfigFile")
	if _, err := s.strategyMgn.GetAuthChecker().CheckConsolePermission(authCtx); err != nil {
		return api.NewConfigFileImportResponseWithMessage(convertToErrCode(err), err.Error())
	}

	ctx = authCtx.GetRequestContext()
	ctx = context.WithValue(ctx, utils.ContextAuthContextKey, authCtx)
	return s.targetServer.ImportConfigFile(ctx, configFiles, conflictHandling)
}
