# Crescendo

[English](README.md)

Crescendo 是运行在 Cadence Maestro 里的引导式电路优化器。勾选它可以调的器件参数、写下什么叫“更好”，它会自动仿真、比较，收敛到最优设计和 Pareto 前沿。原设计全程只读。

![Crescendo 运行仪表盘（合成演示数据）](docs/screenshots/dashboard.png)

## 能做什么

- **六步流程**：Design → Parameters → Goals → Run → Results → Trade-offs，就是左侧导航，完成一步打一个勾。
- **简洁 / 专业两种模式**：默认只显示主流程；Pro mode 打开私有套件、历史结果、可选的 PDK 范围检查、算法参数、执行与提交命令、EDA 覆盖和三级链路诊断。切换只改显示，不改设置。
- **搜索算法**：NSGA-II（两个及以上目标时自动使用）、差分进化、贝叶斯优化；目标和约束可按角点选择，支持 `max` / `mean` 聚合。
- **执行**：Spectre 或 ALPS，本地或批处理集群；每轮使用私有的 Maestro 副本，每个候选完成后写断点，可停止后续跑。
- **实时结果**：指标卡、每个目标的历代最优收敛曲线、候选表、点击即跳转的 Pareto 图，可导出 CSV 和完整数据集。
- **大工程**：在 4000 个模块、50,726 个参数的合成工程上，参数树渲染期间界面最长无响应 74 ms。

## 环境要求

- Python 3.6 及以上（只用标准库）、Tk 8.5 及以上，以及 X 显示。不需要 pip 包或虚拟环境。
- 真实优化需要你自己的 Cadence Virtuoso（已在 IC 6.1.8 上测试）、Maestro 与 Ocean、Spectre 或 ALPS、你的 PDK 和 license。批处理集群可选。
- 演示模式不需要任何 EDA 工具。

## 快速开始（无需 EDA）

```bash
git clone <本仓库> crescendo
cd crescendo
python3 -m crescendo.demo_main --scale small --autoload
```

演示服务生成合成的运放式指标（带固定种子的解析双目标前沿），不调用 Cadence。`--scale large`、`--scale huge` 会生成更大的工程（最多 4000 个模块、约 5 万个参数），用于检查界面响应。

## 在 Cadence 中使用

1. 把发布包解压到运行 Virtuoso 的机器上。
2. 在项目实际加载的 `.cdsinit` 里（先备份）加一行：

   ```lisp
   load("/path/to/crescendo/load.il")
   ```

   如需指定模型根目录，可在这行之前加 `setShellEnvVar("PDK=/path/to/model_root")`。
3. 重启 Virtuoso，在 **Maestro → Plug-in → Open Crescendo Optimizer** 打开（没有 Plug-in 菜单时用顶部 **Crescendo** 菜单）。

Crescendo 读取打开它的那个 Maestro 窗口。工具、模型目录和仿真器从该窗口的环境自动识别，也可以在 **Settings → EDA**（Pro mode）中覆盖。

集群模式在 Pro 模式的 EDA 执行设置或 `runtime.execution_commands` 中配置三条 argv 模板：
`submission_template` 必须包含 `{job_script}`，`scheduler_query_template`、
`scheduler_cancel_template` 各包含一个 `{job_id}`。查询或取消模板为空会阻断集群准备；
本地执行无需这些模板。无默认调度器路径，也不推断选项；复杂逻辑请放到用户包装脚本中。
例如 `batch-submit {job_script}`、`batch-query {job_id}`、`batch-cancel {job_id}` 仅为示意。
提交日志可用 `{scheduler_stdout}` / `{scheduler_stderr}` 绑定到私有任务路径。

通用“调度器键值协议”：查询成功时输出当前作业的 `JOB_ID`（纯数字）和 `JOB_STATE`，
键值以空白、冒号或等号分隔。可选 `EXIT_CODE`（整数或 `-`）、`EXEC_NODE`（如 `node01`）、
`SUBMIT_NODE`、`JOB_NAME`、`QUEUE`、`ACCOUNT`、`REQ_CPU`、`REQ_MEM`、`JOB_CREATE_TIME`、`EXIT_MESSAGE`。
例如 `JOB_ID 10000001` 和 `JOB_STATE RUNNING` 各占一行。支持 JSON 和表格形式；
`TASK[n].STATE` 存在时必须全部终止才可证明作业终止。冲突、其他作业号、消失、查询失败和
取消确认均不能证明停止。求解成功仍需本次任务的回执和结果证据。完整模板、状态及提交输出示例见英文说明。

## 给 agent 的命令行

在仓库根目录运行：

```bash
python3 -m crescendo --version                 # Crescendo 与引擎版本
python3 -m crescendo doctor                    # JSON 环境报告
python3 -m crescendo gui                       # 打开窗口（独立模式）
python3 -m crescendo run optimization-spec.json  # 无界面优化
python3 -m crescendo resume <checkpoint>       # 续跑已停止的运行
python3 -m crescendo parse-rdb <path>          # 把 Maestro 结果库读成 JSON
cadence/crescendo_launch --check               # JSON 启动预检（Python/Tk 探测）
python3 -m crescendo.demo_smoke --scales small large   # 脚本化端到端演示，输出耗时
python3 scripts/run_tests.py                   # 引擎测试
```

## 限制

- 优化效果和耗时取决于你的仿真器、模型和 Maestro 设置；演示数据是合成的，不代表任何真实电路。
- Crescendo 不会把优化值写回你的原始原理图；每轮保留结果、网表和参数证据。
- Global 与 Local 设计变量各自只能是单值，需要多值扫描时放在角点里。
- 范围检查是可选的，只核对每个范围的两个端点。
- 真实站点（你的 ALPS/批处理集群环境、你的 PDK）的验收需要你自己完成；本仓库只在本机用合成夹具测试。

## RF-FIELDWORK

产品介绍页：https://rf-fieldwork.pages.dev/crescendo/?lang=zh（含配音演示片的工具在页面上可直接观看）

其他开源工具：

- [Flux Studio](https://github.com/chloeyntu-byte/flux-studio)：参数化射频无源器件、EMX 扫参、多目标优化与 SNPFit 拟合
- [Cap Array Lab](https://github.com/chloeyntu-byte/cap-array-lab)：电容阵列 C/Q、寄生路径、谐振与 Z11
- [Layout 3D Viewer](https://github.com/chloeyntu-byte/layout-3d-viewer)：三维阅读版图、定位 pin、追踪连接
- [VCO ISF Explorer](https://github.com/chloeyntu-byte/vco-isf-explorer)：ISF、闪烁噪声上变频与相噪贡献

## 许可

MIT，见 [LICENSE](LICENSE)。

