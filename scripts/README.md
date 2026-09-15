# 发布流程

面向维护者。使用者的安装说明在 `README_zh.md`。

## 为什么有这个脚本

发布一个版本意味着**六件事**保持一致：`pyproject.toml` 的版本号、`CHANGELOG.md` 的带日期章节、两个 README 里钉住的版本、git 签名标签、registry 里的产物、以及带附件的 Gitea Release。

`linkerbot-hand` 与 `linkerbot-hand-ros` 的 2.0.0 只做对了前两件，后三件全漏，得到「版本发了却没人装得上」的状态；补齐之后又漏了第六件，成了两个仓库里唯一没有 Release 的标签。这些都不是「更仔细一点」能解决的，所以做成机械校验。

## 分工

**打标签是人的动作，上传是自动化的动作。** `release.py tag` 推送签名标签，标签推送触发 `.gitea/workflows/release.yml` 执行 `release.py publish`。两者跑的是同一个脚本，因此只有一套门禁。

runner 当前离线，工作流推送后只会排队不执行，所以现阶段两步都在本地跑。runner 上线后只需要跑 `tag`。

`publish` 是幂等的：registry 里已有该版本时跳过上传，Release 已存在时跳过创建，因此本地跑过之后 CI 再跑一遍不会重复发布。

## publish 必须从标签构建

`publish` 要求 `v<版本>` 存在**且正好指向 HEAD**。这是 `tag` 那道门禁的反面，是刻意的：标签的全部意义就是记录 registry 里那个产物由哪份源码构建，从别的提交构建就把它唯一的作用毁掉了。

所以本地发布的顺序是：

```bash
python scripts/release.py tag            # 在 main 上
git checkout v<版本>
python scripts/release.py publish
```

CI 里工作流直接 checkout 标签，这道门禁天然满足。

这一条来自 hand 脚本上的一个真实缺陷：那份 `publish` 为了适配 CI 的 detached checkout，把工作区与标签两道门禁整个跳过了，结果本地跑 `publish` 时没有任何门禁把构建的源码与标签关联起来 —— 实测在 `HEAD` 领先标签 1 个提交、且有未提交改动的情况下四道门禁全绿放行，会上传一份在 git 任何提交中都不存在的源码。registry 的版本不能覆盖，所以不可挽回。

## 三个子命令

```bash
python scripts/release.py check      # 只读，不改动任何东西
python scripts/release.py tag        # check 通过后打签名标签并推送
python scripts/release.py publish    # 构建、校验产物、上传、创建 Release
```

凭据从环境变量读，不写进仓库：

```bash
export GITEA_USERNAME=<用户名>
export GITEA_TOKEN=<需要 write:package 与仓库写权限的 token>
```

CI 里从 Actions secret 取同名的两个值。**不要复用全权限 token。**

## 门禁清单

`check` 会逐条校验，任一条失败即停且不产生副作用：

| 门禁 | 拦的是什么 |
| --- | --- |
| 工作区 | 已跟踪文件有改动。未跟踪文件只提示不拦 —— 它们是否会进产物由 sdist 门禁负责 |
| 分支 | 不在 `main`，或与 `origin/main` 不一致（`publish` 不校验这一项，它从标签的 detached checkout 运行） |
| CHANGELOG | 缺少带日期的 `## <版本> - YYYY-MM-DD` 章节；或 `## Unreleased` 章节仍有内容 |
| README | 两个 README 未钉住当前版本，或还钉着旧版本。带 extras 的形式（`linkerbot-transport[can]==X`）与裸形式都会检查 |
| 标签 | `tag` 时：`v<版本>` 已存在。`publish` 时反过来：标签不存在，或不指向 HEAD |
| 质量 | `ruff check` / `ruff format --check` / `pytest` 未通过 |
| registry | 该版本已发布。网络不可达时明确提示「未校验」而非当作「没发布过」—— 否则一次网络故障就会放过一次重复上传 |

`publish` 额外校验产物本身，而不只是「构建成功」：

| 门禁 | 拦的是什么 |
| --- | --- |
| `twine check` | 产物元数据不合规 |
| wheel 版本 | wheel 元数据里的版本与 `pyproject.toml` 不符 |
| wheel 长描述 | `pyproject.toml` 声明 `readme = "README.md"`，README 会打进 wheel 元数据。若 README 钉着旧版本，那份说明会**永久留在已发布的产物里** |
| sdist 内容 | sdist 里出现 git 未跟踪的文件。工作副本里的临时目录肉眼很难发现，而包一旦上传就不该覆盖或回收 |

## 一次发布的完整顺序

1. 改 `pyproject.toml` 的版本号。
2. `CHANGELOG.md` 的 `Unreleased` 内容移入带日期的版本章节，重建空的 `Unreleased`。该章节会成为 Gitea Release 的正文。
3. 两个 README 的安装示例更新到新版本（三处钉法：`[can]==`、`[canfd]==`、裸 `==`）。
4. 上述三步走正常的 Issue + PR 流程并合并。
5. `python scripts/release.py check` —— 确认全绿。
6. `python scripts/release.py tag`
7. `git checkout v<版本>` 然后 `python scripts/release.py publish`（runner 上线后由 CI 完成）

第 4 步必须在 6、7 之前：标签会指向当前提交，而 README 会打进 wheel。顺序颠倒的话，标签指向文档错误的提交，上传的包永久带着旧版本的安装说明。
