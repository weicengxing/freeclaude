# Workspace Agent Prototype

这是一个和主项目隔离的 agent 原型，全部放在 `agent/` 目录下，便于单独测试。

## 当前能力

- 选择本地工作目录
- 持续多轮交互
- 目录内工具
  - `list_dir`
  - `read_file`
  - `search_text`
  - `write_file`
  - `run_command`
- 用户显式授权
  - 读目录默认开启
  - 写文件默认关闭
  - 终端默认关闭

## 运行方式

在项目根目录执行：

```powershell
uvicorn agent.app:app --reload --port 8010
```

然后打开：

```text
http://localhost:8010
```

## 说明

- 这是独立原型，不会改主项目现有聊天页。
- agent 不是“模型直接连本地磁盘”，而是“模型决定下一步用什么工具，后端执行工具，再把结果回喂给模型”。
- 所有工具路径都被限制在你选择的 workspace root 之内。
- `run_command` 会在工作目录内执行，默认超时上限 30 秒。

## 后续可继续加

- 文件 diff / patch 编辑
- 更细粒度命令审批
- 命令流式输出
- 多 agent / plan 面板
