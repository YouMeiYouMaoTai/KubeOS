/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
 * KubeOS is licensed under the Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *     http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
 * PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

use anyhow::Result;
use env_logger::{Builder, Env, Target};
use futures::StreamExt;
use kube::{
    api::{Api, ListParams},
    client::Client,
    runtime::controller::{Context, Controller},
};
use log::{error, info};
// use std::sync::Arc;
use tokio::signal;

mod controller;
use controller::{
    error_policy, reconcile, ControllerClient, OperatorController, OS, SOCK_PATH,
};

const OPERATOR_VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");


#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志记录器，默认日志级别为info，输出到标准输出
    Builder::from_env(Env::default().default_filter_or("info")).target(Target::Stdout).init();
    
    // 创建一个默认的Kubernetes客户端
    let client = Client::try_default().await?;

    // 创建一个用于操作OS资源的API对象，自定义的CR
    let os: Api<OS> = Api::all(client.clone());

    // 创建一个控制器客户端
    let controller_client = ControllerClient::new(client.clone());
    // let controller_client = Arc::new(ControllerClient::new(client.clone()));

    // 创建一个OSReconciler实例
    let os_reconciler = OperatorController::new(client.clone(), controller_client.clone());

    // 记录操作符版本和启动信息
    info!(
        "os-operator version is {}, starting operator manager",
        OPERATOR_VERSION.unwrap_or("Not Found")
    );

    // 创建一个新的控制器并运行，处理reconcile和error_policy，并记录错误信息
    Controller::new(os, ListParams::default())
        .run(reconcile, error_policy, Context::new(os_reconciler))
        .for_each(|res| async move {
            match res {
                Ok(_) => {}
                Err(e) => error!("reconcile failed: {}", e.to_string()),
            }
        })
        .await;

    // 等待终止信号
    signal::ctrl_c().await?;
    info!("os-operator terminated");

    Ok(())
}
