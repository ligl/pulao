# Struxis Contribution Checklist

本清单用于约束 Struxis 的模块边界、改动流程与质量门禁，避免结构回退到跨层耦合。

## 1) 提交前分类（必须）

- [ ] 本次改动属于哪一层：`core` / `market` / `analysis` / `execution`
- [ ] 是否新增模块（文件）
- [ ] 是否涉及跨层依赖变更

## 2) 模块归属规则（必须）

- `constant.rs / events.rs / logging.rs / utils.rs`：基础设施模块；不得依赖业务层。
- `market/*`：行情接入、tick/bar、symbol、indicator；仅可依赖 `core`。
- `analysis/*`：mtc/swing/trend/keyzone/sd；可依赖 `core + market`。
- `execution/*`：decision/strategy；可依赖 `core + market + analysis`。

禁止项：

- [ ] `market -> analysis` 反向依赖
- [ ] `analysis -> execution` 反向依赖
- [ ] 用“临时公共模块”绕过分层

## 3) 接口与兼容性检查

- [ ] 对外导出（`src/lib.rs`）是否需要新增/调整
- [ ] 旧 API 是否被破坏（若破坏，是否给出迁移说明）
- [ ] 文档映射路径是否同步更新（`docs/rust-migration.md`）

## 4) 测试与验证（必须）

- [ ] `cargo check` 通过
- [ ] `cargo test` 通过
- [ ] 若改动涉及回溯逻辑：新增或更新对应回归测试
- [ ] 若改动涉及事件链路：确认 `backtrack_id` 传播语义不变

建议命令：

```bash
cargo check
cargo test -q
```

## 5) 结构类改动附加要求

若提交包含“目录重构/模块迁移”：

- [ ] 不引入行为变化（测试结果与预期一致）
- [ ] import 路径统一到当前语义分层（`constant|events|logging|utils + market/analysis/execution`）
- [ ] 清理旧路径兼容层（如本次目标是语义收敛）

## 6) 变更说明模板（PR 描述建议）

```text
[Layer] core|market|analysis|execution
[Type] feature|refactor|fix|docs
[Scope] 影响模块
[Compatibility] breaking/non-breaking
[Validation] cargo check / cargo test / 关键回归测试
```
