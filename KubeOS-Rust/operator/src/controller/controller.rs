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

use std::{collections::HashMap, env};

use anyhow::Result;
use futures::future::{ok, OrElse};
use k8s_openapi::{api::{core::v1::{DaemonEndpoint, Node}, node}, chrono::Datelike};
use kube::{
    api::{Api, ListParams, ObjectList, PostParams},
    core::{object::HasSpec, ErrorResponse},
    runtime::controller::{Context, ReconcilerAction},
    Client, ResourceExt,
};
use log::{debug, error, info};
use reconciler_error::Error;

use crate::controller::values::NODE_STATUS_UPGRADE;

use super::{
    apiclient::ApplyApi,
    crd::{Configs, Content, OSInstance, OS},
    values::{
        LABEL_MASTER, LABEL_UPGRADING, NODE_STATUS_CONFIG, NODE_STATUS_IDLE, 
        NO_REQUEUE, OPERATION_TYPE_CONFIG, OPERATION_TYPE_ROLLBACK, OPERATION_TYPE_UPGRADE, 
        REQUEUE_ERROR, REQUEUE_NORMAL, SYS_CONFIG_NAME, UPGRADE_CONFIG_NAME, OSI_STATUS_NAME
    },
};

#[derive(Clone)]
pub struct OperatorController<T: ApplyApi> {
    k8s_client: Client,
    controller_client: T,
}

impl<T: ApplyApi> OperatorController<T> {
    pub fn new(k8s_client: Client, controller_client: T) -> Self {
        OperatorController {
            k8s_client,
            controller_client,
        }
    }

    async fn reconcile(
        &self,
        os: OS,
        ctx: Context<OperatorController<T>>,
    ) -> Result<ReconcilerAction, Error> {
        // k8s_client 变量不是 Option 类型，而是直接的 Client 类型，Rust 会确保不为空，不用检查
        
        // Kube-rs库中的Context类型已经处理了上下文的管理，也不需要初始化空的上下文，proxy组件的rust版本也没初始化

        // 调用外部的Reconcile函数
        reconcile(os, ctx).await
    }

    // 获取 worker 节点数
    async fn get_and_update_os(&self, namespace: &str) -> Result<i64, Error> {

        // 创建一个筛选标签的 String 数组，只找 worker 节点
        let reqs = vec![
            format!("!{}", LABEL_MASTER),
        ];

        // 调用getNodes方法获取符合该要求的节点。即worker节点，limit == 0 表示不限制返回数量
        let nodes_items = self.get_nodes( 0, reqs).await?;

        Ok(nodes_items.items.len() as i64)
    }

    // 获取 worker 节点，传入 reqs 为 String 数组，表示多个筛选条件
    async fn get_nodes(&self, limit: i64, reqs: Vec<String>) -> Result<ObjectList<Node>, Error> {
        
        let nodes_api: Api<Node> = Api::all(self.k8s_client.clone());

        // 将多个标签筛选器组合成一个字符串，用逗号分隔
        let label_selector = reqs.join(",");

        // 设置 ListParams
        let list_params = ListParams::default()
            .labels(&label_selector)
            .limit(limit as u32);

        let nodes = match nodes_api.list(&list_params).await {
            Ok(nodes) => nodes,
            Err(e) => {
                log::error!("{:?} unable to list nodes with requirements", e);
                return Err(Error::KubeClient { source: e });
            },
        };

        Ok(nodes)
    }

    // 获取可以进行升级操作的最大节点数量
    async fn check_upgrading(&self, namespace: &str, max_unavailable: i64) -> Result<i64, Error> {

        // 设置筛选标签，选择标签为正在升级的节点
        let reqs = vec![
            LABEL_UPGRADING.to_string(),
        ];

        // 调用getNodes方法获取符合该要求的节点。即worker节点，limit == 0 表示不限制返回数量
        let nodes_items = self.get_nodes( 0, reqs).await?;
        
        Ok(max_unavailable - nodes_items.items.len() as i64)
    }

    // 为指定数量的节点进行升级操作
    async fn assign_upgrade(&self, os: &OS, limit: i64, namespace: &str) -> Result<bool, Error> {
        
        // 创建筛选标签，只找 worker 节点，并且标签 LabelUpgrading 不存在
        let reqs = vec![
            format!("!{}", LABEL_UPGRADING),
            format!("!{}", LABEL_MASTER),
        ];

        // 获取符合标签要求的节点
        let mut nodes_items = self.get_nodes( limit + 1, reqs).await?;

        // 对选定的节点进行升级，返回升级成功的数量和可能的错误
        let count = self.upgrade_nodes(os, &mut nodes_items, limit, namespace).await?;

        Ok(count >= limit)
    }

    async fn upgrade_nodes(&self, os: &OS, nodes: &mut ObjectList<Node>, limit: i64, namespace: &str) -> Result<i64, Error> {

        let mut count = 0;

        for node in nodes.iter_mut() {
            // 如果已经达到升级限制，则退出循环
            if count >= limit {
                break
            }

            let os_version_node = node.status.clone().unwrap().node_info.unwrap().os_image;

            // 检查 os 对象中的操作系统版本是否与节点的操作系统版本不同
            if os_version_node != os.spec.osversion {
                
                // 尝试获取该节点上的 os 实例
                let osi_api: Api<OSInstance> = Api::namespaced(self.k8s_client.clone(), namespace);
                match osi_api.get(&node.name().clone()).await {
                    Ok(mut osi) => {
                        debug!("osinstance is exist {:?}", osi.name());

                        match self.update_node_and_osins(os, node, &mut osi).await {
                            Ok(_) => {
                                count += 1;
                            },
                            Err(_) => {
                                continue;
                            },
                        }
                    },
                    Err(kube::Error::Api(ErrorResponse { reason, .. })) if &reason == "NotFound" => {
                        debug!("failed to get osInstance {}", &node.name().clone());
                        
                        return Err(Error::KubeClient {
                                    source: kube::Error::Api(ErrorResponse { 
                                        reason, 
                                        status: "".to_string(), 
                                        message: "".to_string(), 
                                        code: 0 
                                    })});
                    },
                    Err(_) => continue,
                }
            }

        }

        Ok(count)
    }

    // 升级节点以及节点上的 OSinstance
    async fn update_node_and_osins(&self, os: &OS, node: &mut Node, osinstance: &mut OSInstance, ) -> Result<(), Error> {
        debug!("start update_node_and_OSins");

        // 检查os实例中的升级配置版本与os对象中的升级配置版本是否匹配
        if osinstance.spec.upgradeconfigs.clone().unwrap().version.unwrap() != os.spec.upgradeconfigs.clone().unwrap().version.unwrap() {
            self.deep_copy_spec_configs(os, osinstance, UPGRADE_CONFIG_NAME.to_string()).await?;
        }

        if osinstance.spec.sysconfigs.clone().unwrap().version.unwrap() != os.spec.sysconfigs.clone().unwrap().version.unwrap() {
            self.deep_copy_spec_configs(os, osinstance, SYS_CONFIG_NAME.to_string()).await?;

            if let Some(sysconfigs) = osinstance.spec.sysconfigs.as_mut() {
                if let Some(configs) = &mut sysconfigs.configs {
                    for config in configs {
                        if config.model.clone().unwrap() == "grub.cmdline.current" {
                            config.model = Some("grub.cmdline.next".to_string());
                        }
                        else if config.model.clone().unwrap() == "grub.cmdline.next" {
                            config.model = Some("grub.cmdline.current".to_string());
                        }
                    }
                }
            }
            
        }

        // 更新os实例中的状态为升级完成状态
        osinstance.spec.nodestatus = NODE_STATUS_UPGRADE.to_string();
        
        // 把对 osinstance 的更改从内存更新到 k8s 集群
        let namespace = osinstance.namespace().ok_or(Error::MissingObjectKey {
            resource: String::from("osinstance"),
            value: String::from("namespace"),
        })?;
        self.controller_client.update_osinstance_spec(&osinstance.name(), &namespace, &osinstance.spec).await?;

        node.labels_mut().insert(LABEL_UPGRADING.to_string(), "".to_string());

        // 把对 node 的更改从内存更新到 k8s 集群
        let node_api: Api<Node> = Api::all(self.k8s_client.clone());
        node_api.replace(&node.name(), &PostParams::default(), &node).await?;

        Ok(())
    }

    // 深拷贝
    async fn deep_copy_spec_configs(&self, os: &OS, os_instance: &mut OSInstance, config_type: String) -> Result<(), Error> {
        
        match config_type.as_str() {
            UPGRADE_CONFIG_NAME =>{

                if let Ok(data) = serde_json::to_vec(&os.spec.upgradeconfigs){
                    
                    if let Ok(upgradeconfigs) = serde_json::from_slice(&data) {
                        os_instance.spec.upgradeconfigs = Some(upgradeconfigs);
                    }else {
                        debug!("{} Deserialization failure", config_type);
                        return Err(Error::Operation { value: "Deserialization".to_string()});
                    }
                }
                else {
                    debug!("{} Serialization failure", config_type);
                    return Err(Error::Operation { value: "Serialization".to_string()});
                }

            },
            SYS_CONFIG_NAME => {

                if let Ok(data) = serde_json::to_vec(&os.spec.sysconfigs){
                    
                    if let Ok(sysconfigs) = serde_json::from_slice(&data) {
                        os_instance.spec.sysconfigs = Some(sysconfigs);
                    }else {
                        debug!("{} Deserialization failure", config_type);
                        return Err(Error::Operation { value: "Deserialization".to_string()});
                    }
                }
                else {
                    debug!("{} Serialization failure", config_type);
                    return Err(Error::Operation { value: "Serialization".to_string()});
                }

            },
            _ => {
                debug!("configType {} cannot be recognized", config_type);
                return Err(Error::Operation { value: config_type.clone() });
            },
        }
        
        Ok(())
    }

    // 获取可以进行配置操作的节点数量，返回的是 instance 的列表
    async fn check_config(&self, namespace: &str, max_unavailable: i64) -> Result<i64, Error> {
        let osinstances = self.get_config_osinstances(namespace).await?;

        Ok(max_unavailable - osinstances.items.len() as i64)
    }

    // 获取所在节点状态为配置的 osinstance列表
    async fn get_config_osinstances(&self, namespace: &str) -> Result<ObjectList<OSInstance>, Error> {
        
        let osi_api: Api<OSInstance> = Api::namespaced(self.k8s_client.clone(), namespace);

        let lp = ListParams::default()
            .fields(&format!("{}={}", OSI_STATUS_NAME, NODE_STATUS_CONFIG));

        let osinstances =  match osi_api.list(&lp).await {
            Ok(os_instance_list) => os_instance_list,
            Err(err) => {
                debug!("err: {:?}", err.to_string());
                log::error!("unable to list nodes with requirements: {}", err);
                return Err(Error::KubeClient { source: err });
            }
        };

        Ok(osinstances)
    }

    // 为指定数量的节点进行配置操作
    async fn assign_config(&self, os: &OS, sysconfigs: Configs, config_version: String, limit: i64, namespace: &str) -> Result<bool, Error> {

        let mut osinstances = self.get_idle_osInstances(namespace, limit + 1).await?;

        let mut count = 0;
        // 遍历 osi 列表
        for osi in osinstances.iter_mut() {
            if count >= limit {
                break;
            }

            // 获取节点的操作系统版本
            let config_version_node = osi.spec.sysconfigs.clone().unwrap().version.unwrap();
        
		    // 如果版本不同，则将新的配置信息更新到实例中，并将节点状态标记为“配置完成”。
            if config_version_node != config_version {
                count += 1;
                osi.spec.sysconfigs = Some(sysconfigs.clone());
                osi.spec.nodestatus = NODE_STATUS_CONFIG.to_string();
                
                // 把对 osinstance 的更改从内存更新到 k8s 集群
                let namespace = osi.namespace().ok_or(Error::MissingObjectKey {
                    resource: String::from("osinstance"),
                    value: String::from("namespace"),
                })?;
                self.controller_client.update_osinstance_spec(&osi.name(), &namespace, &osi.spec).await?;
            }
        }
        
        Ok(count >= limit)
    }

    // 获取所在节点状态为配置的 osinstance列表
    async fn get_idle_osInstances(&self, namespace: &str, limit: i64) -> Result<ObjectList<OSInstance>, Error> {
        
        let osi_api: Api<OSInstance> = Api::namespaced(self.k8s_client.clone(), namespace);

        let lp = ListParams::default()
        .fields(&format!("{}={}", OSI_STATUS_NAME, NODE_STATUS_IDLE))
        .limit(limit as u32);

        let osinstances =  match osi_api.list(&lp).await {
            Ok(os_instance_list) => os_instance_list,
            Err(err) => {
                log::error!("unable to list nodes with requirements: {}", err);
                return Err(Error::KubeClient { source: err });
            }
        };

        Ok(osinstances)
    }


}

// 调用的函数的具体逻辑需要进一步完成
pub async fn reconcile<T: ApplyApi>(
    os: OS,
    ctx: Context<OperatorController<T>>,
) -> Result<ReconcilerAction, Error> {

    // 初始化 operator_controller 和 os，从环境变量获取NODE_NAME
    debug!("start reconcile");
    let operator_controller = ctx.get_ref();
    let os_cr: &OS = &os;

    // 从 os_cr 中获取命名空间，如果命名空间不存在则返回错误
    let namespace: String = os_cr
        .namespace()
        .ok_or(Error::MissingObjectKey { resource: "os".to_string(), value: "namespace".to_string() })?;

    // 获取 worker 节点数
    let node_num = match operator_controller.get_and_update_os(&namespace).await {
        Ok(node_num) => node_num,
        Err(Error::KubeClient { source: kube::Error::Api(ErrorResponse { reason, .. })}) if &reason == "NotFound" => {
            return Ok(NO_REQUEUE);
        },
        Err(_) => return Ok(REQUEUE_ERROR),
    };

    let opstype = os_cr.spec.opstype.clone();
    let ops = opstype.as_str();
    
    debug!("opstype: {}", ops);

    match ops {
        // 如果是升级或者回滚
        OPERATION_TYPE_UPGRADE | OPERATION_TYPE_ROLLBACK =>{
            debug!("start upgrade OR rollback");

            // 获取可以进行升级操作的最大节点数量
            let limit = operator_controller.check_upgrading(&namespace, os_cr.spec.maxunavailable.min(node_num)).await?;

            debug!("limit: {}", limit);

            // 为指定数量的节点进行升级操作
            let need_requeue = operator_controller.assign_upgrade(os_cr, limit, &namespace).await?;
            if need_requeue {
                return Ok(REQUEUE_NORMAL);
            }
        },
        // 配置操作
        OPERATION_TYPE_CONFIG =>{
            debug!("start config");

            // 检查待配置的节点数量
            let limit = operator_controller.check_config(&namespace, os_cr.spec.maxunavailable.min(node_num)).await?;

            // 指派配置任务给节点
            let sys_configs = os_cr.spec.sysconfigs.clone().unwrap();
            let version = os_cr.spec.sysconfigs.clone().unwrap().version.unwrap();
            let need_requeue = operator_controller.assign_config(os_cr, sys_configs, version, limit, &namespace).await?;

            if need_requeue {
                return Ok(REQUEUE_NORMAL);
            }
        },
        _ =>{
            log::error!("operation {} cannot be recognized", ops);
        }
    }
    return Ok(REQUEUE_NORMAL);
}

pub fn error_policy<T: ApplyApi>(
    error: &Error,
    _ctx: Context<OperatorController<T>>,
) -> ReconcilerAction {
    error!("Reconciliation error: {}", error.to_string());
    REQUEUE_ERROR
}

pub mod reconciler_error {
    use thiserror::Error;

    use crate::controller::{apiclient::apiclient_error};

    #[derive(Error, Debug)]
    pub enum Error {
        #[error("Kubernetes reported error: {source}")]
        KubeClient {
            #[from]
            source: kube::Error,
        },

        #[error("Create/Patch OSInstance reported error: {source}")]
        ApplyApi {
            #[from]
            source: apiclient_error::Error,
        },

        #[error("Cannot get environment NODE_NAME, error: {source}")]
        Env {
            #[from]
            source: std::env::VarError,
        },

        #[error("{}.metadata.{} is not exist", resource, value)]
        MissingObjectKey { resource: String, value: String },

        #[error("Cannot get {}, {} is None", value, value)]
        MissingSubResource { value: String },

        #[error("operation {} cannot be recognized", value)]
        Operation { value: String },

        #[error("Expect OS Version is not same with Node OS Version, please upgrade first")]
        UpgradeBeforeConfig,

        #[error("Error when drain node, error reported: {}", value)]
        DrainNode { value: String },
    }
}

#[cfg(test)]
mod test {
    use std::{borrow::Borrow, cell::RefCell, env};

    use serde::de;

    use super::{error_policy, reconcile, reconciler_error::Error, Context, OSInstance, OperatorController, OS};
    use crate::controller::{
        apiserver_mock::{timeout_after_5s, K8sResources, Testcases},
        ControllerClient,
    };
    
    #[tokio::test]
    async fn test_rollback() {
        env::set_var("RUST_LOG", "debug");
        env_logger::init();

        let (test_operator_controller, fakeserver) = OperatorController::<ControllerClient>::test();
        let os = OS::set_os_rollback_osversion_v1_upgradecon_v1();
        let context = Context::new(test_operator_controller);
        let mocksrv = fakeserver
            .run(Testcases::Rollback(K8sResources::set_rollback_nodes_v2_and_osi_v1()));
        reconcile(os, context.clone()).await.expect("reconciler");
        timeout_after_5s(mocksrv).await;
    }

    #[tokio::test]
    async fn test_config_normal() {
        env::set_var("RUST_LOG", "debug");
        env_logger::init();

        let (test_operator_controller, fakeserver) = OperatorController::<ControllerClient>::test();
        let os = OS::set_os_syscon_v2_opstype_config();
        let context = Context::new(test_operator_controller);
        let mocksrv = fakeserver
            .run(Testcases::ConfigNormal(K8sResources::set_nodes_v1_and_osi_v1()));
        reconcile(os, context.clone()).await.expect("reconciler");
        timeout_after_5s(mocksrv).await;
    }

    #[tokio::test]
    async fn test_skip_no_osi_node() {
        env::set_var("RUST_LOG", "debug");
        env_logger::init();

        let (test_operator_controller, fakeserver) = OperatorController::<ControllerClient>::test();
        let os = OS::set_os_skip_osversion_v2_upgradecon_v1();
        let context = Context::new(test_operator_controller);
        let mocksrv = fakeserver
            .run(Testcases::SkipNoOsiNode(K8sResources::set_skip_nodes_and_osi()));
        reconcile(os, context.clone()).await.expect("reconciler");
        timeout_after_5s(mocksrv).await;
    }

    #[tokio::test]
    async fn test_exchange_current_and_next() {
        env::set_var("RUST_LOG", "debug");
        env_logger::init();

        let (test_operator_controller, fakeserver) = OperatorController::<ControllerClient>::test();
        let os = OS::set_os_exchange_current_and_next();
        let context = Context::new(test_operator_controller);
        let mocksrv = fakeserver
            .run(Testcases::ExchangeCurrentAndNext(K8sResources::set_nodes_v1_and_osi_v1()));
        reconcile(os, context.clone()).await.expect("reconciler");
        timeout_after_5s(mocksrv).await;
    }

    #[tokio::test]
    async fn test_deep_copy_spec_configs() {
        env_logger::init();

        let (test_operator_controller, fakeserver) = OperatorController::<ControllerClient>::test();
        let deep_copy_result = test_operator_controller.clone().deep_copy_spec_configs(&OS::set_os_default(), &mut OSInstance::set_osi_default("", ""), "test".to_string()).await;
        
        assert!(deep_copy_result.is_err());
        
        if let Err(err) = deep_copy_result {
            assert_eq!("operation test cannot be recognized".to_string(), err.borrow().to_string());
        }
    }

    #[tokio::test]
    async fn test_get_config_osinstances() {
        env_logger::init();

        let (test_operator_controller, fakeserver) = OperatorController::<ControllerClient>::test();

        let expected_error = "list error".to_string();

        fakeserver.test_function(Testcases::GetConfigOSInstances(expected_error.clone()));

        // 执行测试
        let result = test_operator_controller.get_config_osinstances("default").await;

        // 验证返回值
        assert!(result.is_err());
        if let Err(err) = result {
            match err {
                Error::KubeClient { source } => {
                    match source {
                        kube::Error::Api(error_response) => {
                            assert_eq!(expected_error, error_response.message);
                        },
                        _ => {
                            assert!(false);
                        }
                    }
                }
                _ => {
                    assert!(false);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_check_upgrading() {
        env_logger::init();

        let (test_operator_controller, fakeserver) = OperatorController::<ControllerClient>::test();

        fakeserver.test_function(Testcases::CheckUpgrading("label error".to_string()));

        // 执行测试
        let result = test_operator_controller.check_upgrading("default", 2).await;

        // 验证返回值
        assert!(result.is_err());
        if let Err(err) = result {
            match err {
                Error::KubeClient { source } => {
                    match source {
                        kube::Error::Api(error_response) => {
                            assert_eq!("label error", error_response.message);
                        },
                        _ => {
                            assert!(false);
                        }
                    }
                }
                _ => {
                    assert!(false);
                }
            }
        }
    }


    #[tokio::test]
    async fn test_get_idle_osinstances() {
        env_logger::init();

        let (test_operator_controller, fakeserver) = OperatorController::<ControllerClient>::test();

        let expected_error = "list error".to_string();

        fakeserver.test_function(Testcases::GetIdleOSInstances(expected_error.clone()));

        // 执行测试
        let result = test_operator_controller.get_idle_osInstances("default", 3).await;

        // 验证返回值
        assert!(result.is_err());
        if let Err(err) = result {
            match err {
                Error::KubeClient { source } => {
                    match source {
                        kube::Error::Api(error_response) => {
                            assert_eq!(expected_error, error_response.message);
                        },
                        _ => {
                            assert!(false);
                        }
                    }
                }
                _ => {
                    assert!(false);
                }
            }
        }
    }

}
