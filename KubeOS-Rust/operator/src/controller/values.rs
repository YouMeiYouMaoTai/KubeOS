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

use kube::runtime::controller::ReconcilerAction;
use tokio::time::Duration;

#[cfg(test)]
pub const LABEL_OSINSTANCE: &str = "upgrade.openeuler.org/osinstance-node";

pub const LABEL_UPGRADING: &str = "upgrade.openeuler.org/upgrading";

pub const LABEL_MASTER: &str = "node-role.kubernetes.io/control-plane";

pub const OSINSTANCE_API_VERSION: &str = "upgrade.openeuler.org/v1alpha1";
pub const OSINSTANCE_KIND: &str = "OSInstance";
// pub const OSI_STATUS_NAME: &str = "nodestatus";

pub const UPGRADE_CONFIG_NAME: &str = "UpgradeConfig";
pub const SYS_CONFIG_NAME: &str = "SysConfig";

pub const NODE_STATUS_IDLE: &str = "idle";
pub const NODE_STATUS_UPGRADE: &str = "upgrade";
pub const NODE_STATUS_CONFIG: &str = "config";

pub const OPERATION_TYPE_UPGRADE: &str = "upgrade";
pub const OPERATION_TYPE_ROLLBACK: &str = "rollback";
pub const OPERATION_TYPE_CONFIG: &str = "config";


pub const NO_REQUEUE: ReconcilerAction = ReconcilerAction { requeue_after: None };

pub const REQUEUE_NORMAL: ReconcilerAction = ReconcilerAction { requeue_after: Some(Duration::from_secs(15)) };

pub const REQUEUE_ERROR: ReconcilerAction = ReconcilerAction { requeue_after: Some(Duration::from_secs(1)) };