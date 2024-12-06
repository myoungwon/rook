---
title: NVMe-oF Recoverer
target-version: release-X.X
---

# NVMe-oF Recoverer

## Summary
We propose a recovery design, called NVMe-oF recoverer. NVMe-oF is an emerging storage network where the storage devices can be attached to any storage node using RDMA, TCP or Fiber channel. In this environment, the failure caused by a server node can be recoverable because the OSD can be re-spawned to another server node with the storage device attached to the failed server node through NVMe-oF.

So, what we propose is a functionality that moves the OSD pod to another server node either automatically or manually when the server node fails because storage devices are still workable in NVMe-oF environment.

### Goals

- **Enhanced Fault Tolerance**: Dynamically reassign storage resources in case of compute node failures. NVMe-oF eliminates the need for physical device movement, providing higher fault tolerance.

- **Minimized Downtime**: Automatically reassign storage devices and redeploy OSDs, reducing manual intervention and ensuring data availability with minimal delay.

- **Load balance** (Future Work): As storage needs grow, Recoverer enables easy expansion of storage capacity. It introduces a scheduling mechanism that dynamically reallocates fabric devices to suitable nodes based on workload. By leveraging load-balancing techniques, the scheduler optimizes device placement and ensures efficient optimal performance.

### Non-Goals

- **Support for Non-NVMe-oF Environments**: This design focuses solely on NVMe-oF environments.
- **Modifications to the Core Ceph Codebase**: The proposal does not involve changing Ceph's core functionalities.

## Proposal details

### Overview

We propose a design, called NVMe-oF recoverer. NVMe-oF is an emerging storage network where the storage devices can be attached to any storage node using RDMA, TCP or Fiber channel. In this environment, the failure caused by a server node can be recoverable because the OSD can be re-spawned to another server node with the storage device attached to the failed server node through NVMe-oF.

The NVMe-oF recoverer will be developed in four steps to address these challenges. **Steps 1 to 3** focus on immediate implementation for core recovery functionalities, while **Step 4** represents a long-term plan for workload optimization.

**1. Implement an Explicit Command for Manual OSD Relocation**
- Create an explicit command to manually relocate OSD pods to another node. This is required because some behaviors such as deleting crashed OSD pod and moving block devices to another host are needed, as operator does not redeploy OSDs that are already created even if there are in a fault state.
  - Usage Example:
    ```bash
    kubectl exec -it rook-ceph-operator-xxxxx -- osd_move node-failed node-healthy
    ```

**2. Automate OSD Relocation Upon Failure**
- Implement an operator that moves OSD pod in the event of OSD fails automatically, including checking recoverable events. The command at **step 1** is replaced with the operator.

**3. Modify CRUSH Map to Remove Data Rebalancing**
- To remove data rebalance after the relocation of OSD, CRUSH map strategy for relocation is required. We can deliver guidance or generate a rule not to trigger the data rebalance.
  - For example, moving an OSD from `HOST 1` to `HOST 2` causes a rebalance because the fault domain is changed.

**4. Improve OSD Scheduling Based on Node Workloads**
- Improve the operator to have a scheduling functionality that balances the number of OSDs each server node has depending on its workloads.


Each step can be implemented and contributed individually, allowing for incremental progress and testing.

### Step 1: Implement an Explicit Command for Manual OSD Relocation

#### Description

Introduce explicit commands or scripts that allow administrators to manually deploy and relocate OSD pods using NVMe-oF devices. These tools will streamline the management of NVMe-oF devices and OSD pods in standard scenarios and during failures.

#### Implementation Steps

- **Develop a Script or Command-line Tool**:
  - **For Connecting NVMe-oF Devices and Deploying OSD Pods**:
    - Retrieve NVMe-oF target information from predefined sources (e.g., configuration files, environment variables).
      - Key parameters: IP, Port, Subsystem NQN (SubNQN), and attachable nodes.
    - Connect devices to target nodes using `nvme-cli` or appropriate tools.
    - Update the `CephCluster` Custom Resource Definition (CRD) with fabric device details to facilitate automated OSD deployment.
  - **For Handling OSD Relocation in Failure Scenarios**:
    - Remove the faulted OSD pod.
    - Disconnect the device from the failed node and reconnect it to a healthy node.
    - Update the `CephCluster` CRD to reflect the new device-node mapping.


#### Benefits

- Provides immediate control for administrators.
- Simplifies the process of OSD deployment and relocation.

### Step 2: Automate OSD Relocation Upon Failure

#### Description

Enhance the Rook operator to automatically detect OSD pod failures and relocate the affected OSDs to healthy nodes, reducing manual intervention and downtime.

#### Implementation Steps

- **Discovery of Connected Fabric Devices**:
  - **Scan for Previously Connected Devices**:
    - During initialization, use Kubernetes Jobs to scan all nodes for previously connected NVMe-oF devices.
    - Perform device discovery using `nvme-cli`.
  - **Identify Devices by SubNQN**:
    - Use SubNQN to verify the connection status of devices and ensure correct node mapping.
- **Detect OSD Pod Failures**:
  - Monitor the state of OSD pods to identify failures associated with NVMe-oF devices.
  - Utilize the operator's reconciler to detect failures and trigger recovery actions.
- **Relocate Failed OSD**:
  - **Delete the Affected OSD Pod**:
    - Upon detecting a failure, remove the OSD pod from the failed node.
  - **Reconnect the NVMe-oF Device**:
    - Disconnect the device from the failed node.
    - Connect the device to a healthy node.
  - **Update the CephCluster CRD**:
    - Reflect the new device-node mapping in the CRD.
    - The operator will automatically deploy the OSD pod on the new node based on the updated CRD.

#### Benefits

- Minimizes downtime by automating recovery processes.
- Reduces the need for manual intervention during node failures.

### Step 3: Modify CRUSH Map to Minimize Data Rebalancing

#### Description

To prevent unnecessary data rebalancing during OSD relocation, we introduce the concepts of `virtual` and `physical` hosts in the CRUSH map.
Note that the virtual host is a logical bucket leveraging existing CRUSH rule to group OSDs using fabric devices into a single failure domain, while a physical host represents OSDs directly attached to a server. Since the virtual host exists as a logical entity in the CRUSH map, the map remains unchanged when an OSD's physical location changes (e.g., moving from Host 1 to Host 2), avoiding recovery I/Os and ensuring consistent data placement.

#### Implementation Steps

- **Update OSD Creation Logic**:
  - Modify the `prepareOSD()` function to assign OSDs to specific CRUSH map locations, such as virtual hosts.
- **Assign OSDs to Virtual Hosts**:
  - During OSD creation (as implemented in Step 2), set the CRUSH map location of OSDs to the corresponding virtual host.
- **Validate with Integration Tests**:
  - Conduct tests to verify that relocating an OSD (e.g., from `node1` to `node2`) does not alter the CRUSH map or trigger unnecessary recovery I/O.
  - Ensure data placement remains consistent and no additional data rebalancing occurs.

#### Benefits

- **Efficient Recovery**: Avoids unnecessary rebalancing, reducing recovery time.

### Step 4: Improve OSD Scheduling Based on Node Workloads

#### Description

Implement a scheduling mechanism within the operator to dynamically balance the number of OSDs across server nodes based on their workloads. This approach leverages the flexibility of NVMe-oF devices to prevent performance bottlenecks and optimize resource utilization.

#### Implementation Steps

- **Design a Scheduling Mechanism**:
  - Define a strategy for monitoring node workloads (e.g., CPU, memory, I/O usage).
  - Determine OSD placement based on collected metrics.
- **Extend the Operator**:
  - Modify the operator to evaluate workload metrics and adjust OSD assignments dynamically.
  - Utilize Kubernetes APIs or monitoring tools to gather metrics.
- **Validate the Scheduling Mechanism**:
  - Conduct tests to ensure effective load balancing and performance optimization.
  - Simulate various workload scenarios to verify the scheduler's responsiveness.

#### Benefits

- **Optimized Performance**: Balances workloads to prevent overloading individual nodes.
- **Scalability**: Facilitates smooth expansion of storage capacity by efficiently utilizing resources.

## Mitigation

- **Data Integrity**:
  - Conduct thorough testing to ensure fail-safe and reversible relocation processes.
- **Performance Optimization**: Use parallel scan jobs for device discovery during reinitialization to reduce overhead.


- **Data Integrity**:
  - Implement thorough testing and validation procedures.
  - Ensure relocation processes are fail-safe and reversible.
- **Performance Optimization**:
  - Use parallel scan jobs for device discovery during reinitialization to reduce overhead.

## Alternatives

- **Manual Management Only**: Relies on administrators for OSD relocation without operator enhancements.
  - **Drawback**: Increases downtime and operational complexity.
