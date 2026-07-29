# 锐龙 APU 平台稳定性调优指南

## 问题背景

在 AMD 锐龙 APU（如 Ryzen 5 5600G、Ryzen 7 5700G 等集成 Radeon Graphics 的处理器）平台上，大量文件 I/O 操作可能导致系统黑屏。根本原因是：

- **页缓存积压**：大量 SSTable 文件读写导致内核页缓存膨胀，压缩显卡驱动（amdgpu）的可用内存
- **内存碎片化**：长期运行后堆内存碎片影响显卡驱动分配连续 DMA 内存
- **GTT 映射表膨胀**：IOMMU 映射表过大或未及时释放，触发驱动保护

## 应用层优化（内置）

kv_store 引擎内置了以下内存管理优化，无需额外配置：

### 1. 维护线程（自动启用）

每 30 秒执行一次：
- `malloc_trim(0)` — 释放堆内存碎片归还给 OS
- `posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED)` — 释放已刷盘 SSTable 文件的内核页缓存

### 2. APU 兼容模式

启动时自动检测 `/proc/cpuinfo` 中的 AMD APU 特征，若检测到则：
- 维护线程间隔缩短为 15 秒
- 可通过环境变量显式控制：`KV_APU_COMPAT=1 ./kv_raft_node ...`

### 3. O_DIRECT 编译选项（可选）

编译时添加 `-DUSE_O_DIRECT=ON` 启用直接 I/O，绕过页缓存读写 SSTable 文件：

```bash
cmake .. -DUSE_O_DIRECT=ON -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

## 系统层调优

以下 sysctl 参数可显著改善 APU 在重 I/O 负载下的稳定性：

| 参数 | 推荐值 | 默认值 | 说明 |
|------|--------|--------|------|
| `vm.min_free_kbytes` | 1048576 (1GB) | 67584 | 保证系统始终保留 1GB 内存供内核紧急使用 |
| `vm.dirty_background_ratio` | 5 | 10 | 更早触发后台回写，避免脏页积压 |
| `vm.dirty_ratio` | 20 | 40 | 限制脏页上限，防止页缓存占用过多内存 |
| `vm.vfs_cache_pressure` | 50 | 100 | 降低内核回收 inode/dentry 缓存的倾向 |

### 临时生效

```bash
sudo sysctl -w vm.min_free_kbytes=1048576
sudo sysctl -w vm.dirty_background_ratio=5
sudo sysctl -w vm.dirty_ratio=20
sudo sysctl -w vm.vfs_cache_pressure=50
```

### 永久配置

编辑 `/etc/sysctl.conf` 或 `/etc/sysctl.d/99-kv-store.conf`：

```ini
vm.min_free_kbytes=1048576
vm.dirty_background_ratio=5
vm.dirty_ratio=20
vm.vfs_cache_pressure=50
```

然后执行 `sudo sysctl -p` 使其生效。

### GRUB 内核参数

在 `/etc/default/grub` 的 `GRUB_CMDLINE_LINUX` 中添加：

```
amdgpu.gttsize=4096
```

然后更新 GRUB：

```bash
sudo update-grub    # Debian/Ubuntu
sudo grub2-mkconfig -o /boot/grub2/grub.cfg  # CentOS/RHEL
```

## 验证方案

### 1. 监控内存使用

```bash
# 监控整体内存
watch -n 5 'cat /proc/meminfo | grep -E "MemFree|MemAvailable|Cached|Dirty"'

# 监控 GTT 使用
watch -n 5 'cat /sys/kernel/debug/dri/0/amdgpu_gtt 2>/dev/null || echo "debugfs not mounted"'
```

### 2. 监控内核日志

```bash
# 实时监控 amdgpu 错误
dmesg -w | grep -i amdgpu

# 检查历史错误
dmesg | grep -i "amdgpu\|gtt\|iommu"
```

### 3. 混沌测试

```bash
# 在应用所有修复后持续运行数小时
python scripts/test_chaos_100k.py --duration 3600 --verbose

# 同时监控内存
watch -n 10 'cat /proc/meminfo | head -5'
```

## 故障排查

| 症状 | 可能原因 | 解决方案 |
|------|---------|---------|
| `amdgpu: gtt allocation failed` | GTT 空间不足 | 增加 `amdgpu.gttsize=4096` |
| 黑屏伴随 I/O 错误 | 页缓存耗尽 DMA 内存 | 应用 sysctl 调优 + 启用 APU 兼容模式 |
| 系统 OOM 后黑屏 | min_free_kbytes 过低 | 设置 `vm.min_free_kbytes=1048576` |
| IOMMU 相关错误 | IOMMU 映射表过大 | 尝试 `amd_iommu=off` 或 `iommu=pt`（谨慎） |

## 编译选项汇总

| 选项 | 说明 | 推荐场景 |
|------|------|---------|
| `-DUSE_O_DIRECT=ON` | 绕过页缓存读写 SSTable | APU 平台，内存紧张 |
| `-DCMAKE_BUILD_TYPE=Release` | 优化编译 | 生产环境 |
| `-DMEMTABLE_SIZE_LIMIT=524288` | 增大 MemTable（512KB） | 高吞吐场景 |