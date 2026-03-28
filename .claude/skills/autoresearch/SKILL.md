---
name: autoresearch
description: Multi-round iterative research using parallel agents. Decomposes research questions into sub-questions, dispatches agents in parallel, evaluates gaps, and iterates until all sub-questions are adequately answered. Inspired by karpathy/autoresearch.
argument-hint: "[research question]"
allowed-tools: "Read, Grep, Glob, Bash(ls *), Agent, WebSearch, WebFetch"
---

# Autoresearch（自主迭代研究）

灵感来源：[karpathy/autoresearch](https://github.com/karpathy/autoresearch) —— 让 AI agent 自主执行研究循环：提出假设 → 收集证据 → 评估结论 → 发现缺口 → 补充研究 → 迭代，直到研究问题被充分回答。

## 核心理念

与一次性搜索不同，autoresearch 是一个**多轮迭代**的研究过程：

```
用户提出研究问题
    ↓
[Round 1] 拆解子问题 → 多 Agent 并行收集 → 汇总初步发现
    ↓
[Round 2] 评估缺口（哪些问题没答好？哪些数据矛盾？）→ 针对性补充搜索
    ↓
[Round N] 重复直到所有子问题都有可靠答案
    ↓
输出最终研究结论
```

## 硬性要求

1. **必须使用多 Agent 并行**：所有可并行的子任务必须用 `Agent(run_in_background=true)` 同时启动，不允许串行执行独立任务。主 Agent 在等待期间处理已有结果，不干等。

## 执行流程

### Step 1：分析研究问题，生成子问题树

收到用户的研究问题后，将其拆解为 3-6 个独立的子问题。子问题应满足：
- **互相独立**：可以并行调查，不依赖彼此的结果
- **完全覆盖**：合起来能完整回答原始问题
- **可验证**：每个子问题都有明确的"答了/没答"标准

```markdown
## 研究问题：{用户问题}

### 子问题拆解
1. {子问题1} — 预期信息源：{年报/网络/数据库}
2. {子问题2} — 预期信息源：{...}
3. {子问题3} — 预期信息源：{...}
...
```

### Step 2：Round 1 — 多 Agent 并行收集

为每个子问题分配一个 Agent，**全部后台并行启动**：

```
Agent(子问题1, run_in_background=true)
Agent(子问题2, run_in_background=true)
Agent(子问题3, run_in_background=true)
...
```

每个 Agent 的 prompt 需包含：
- 明确的子问题描述
- 建议的信息源和搜索策略（本地文件用 Read/Grep，网络信息用 WebSearch/WebFetch）
- 要求的输出格式：`发现 + 置信度 + 来源`

**关键规则**：
- Agent 失败 → 立即重试（换策略），最多 2 次
- 部分完成 → 先用已有结果开始初步分析
- 全部完成 → 确认每个子问题都有对应结果

### Step 3：评估 & 发现缺口

所有 Round 1 Agent 完成后，主 Agent 对结果进行批判性评估：

```markdown
## Round 1 评估

| 子问题 | 状态 | 发现摘要 | 缺口/问题 |
|--------|------|---------|-----------|
| 子问题1 | ✅ 充分 | ... | — |
| 子问题2 | ⚠️ 部分 | ... | 缺少2020年之前的数据 |
| 子问题3 | ❌ 不足 | ... | 搜索结果无相关内容 |

### 需要补充的方向
- {缺口1}：尝试{新的搜索策略}
- {缺口2}：改用{其他数据源}
```

### Step 4：Round 2+ — 针对缺口的补充研究

对 Round 1 中标记为"部分"或"不足"的子问题，启动新一轮 Agent：

```
Agent(缺口1_补充搜索, run_in_background=true)
Agent(缺口2_换数据源, run_in_background=true)
```

补充搜索的策略调整：
- **换关键词**：如果直接搜索无果，尝试同义词、子公司名、产品名
- **换数据源**：如果网络搜索无果，尝试本地财报 PDF；反之亦然
- **缩小范围**：如果结果太泛，增加时间、地区等限定条件
- **扩大范围**：如果结果太少，去掉部分限定条件

### Step 5：最终汇总

当所有子问题都达到"充分"或经过 2 轮补充后仍无法改善时，输出最终结论：

```markdown
## {研究问题} — 研究结论

### 核心发现
1. {发现1}（来源：{...}，置信度：高）
2. {发现2}（来源：{...}，置信度：中）
...

### 数据汇总表
| 维度 | 数据 | 来源 | 年份 |
|------|------|------|------|
| ... | ... | ... | ... |

### 未解决的问题
- {仍然无法确认的点}（原因：{...}）

### 研究过程摘要
- Round 1：{N} 个 Agent 并行，覆盖 {子问题列表}
- Round 2：针对 {缺口} 补充搜索
- 总计 {M} 个 Agent 调用
```

## 适用场景

| 场景 | 子问题拆解示例 |
|------|--------------|
| 某公司历年某指标追踪 | 按年份分组，每 Agent 负责 3-4 年 |
| 行业竞争格局分析 | 按公司分组，每 Agent 负责一家公司 |
| 某公司全面基本面分析 | 按维度分组：财务/产品/客户/管理层/风险 |
| 跨公司横向比较 | 按比较维度分组：产能/市占率/毛利率/技术路线 |
| 政策或事件影响分析 | 按影响维度分组：短期业绩/长期格局/受益方/受损方 |

## 与其他 skill 的配合

- 需要深度信息收集 → Round 1 的某个 Agent 可引用其他研究类 skill 的搜索策略
- 对于 A/H 套利项目 → 子问题可围绕：数据源可靠性、延迟测量、汇率风险、监管限制等维度展开
