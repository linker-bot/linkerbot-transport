# 发布流程

本文面向维护者。发布脚本将项目版本、变更记录、README、签名标签、包仓库和
Gitea Release 作为一组检查，保证发布产物来自已确认的提交。

## 命令

```bash
python scripts/release.py check      # 只读检查
python scripts/release.py tag        # 检查通过后创建并推送签名标签
python scripts/release.py publish    # 从标签构建、校验并发布
```

`tag` 在 `main` 上执行。`publish` 要求 `v<版本>` 已存在且正好指向当前提交；
本地发布时先切换到该标签：

```bash
python scripts/release.py tag
git switch v<版本>
python scripts/release.py publish
```

凭据只从环境变量读取，不写入仓库：

```bash
export GITEA_USERNAME=<用户名>
export GITEA_TOKEN=<需要发布权限的令牌>
```

## 检查内容

`check` 会在产生副作用前检查：

- 工作区没有已跟踪改动，当前分支与 `origin/main` 一致；
- `CHANGELOG.md` 有当前版本日期章节，`Unreleased` 已清空；
- 两个 README 只固定当前版本的安装依赖；
- 当前版本的标签尚不存在，质量检查全部通过；
- 包仓库尚未包含当前版本。

`publish` 还会校验 wheel 元数据、README 长描述、sdist 文件清单和包仓库上传结果，
然后使用当前版本的变更记录创建 Gitea Release。重复执行时，已经存在的包和 Release
会跳过对应操作。

## 发布顺序

1. 更新 `pyproject.toml` 版本号。
2. 将 `CHANGELOG.md` 的 `Unreleased` 内容移入当前版本日期章节，并重建空章节。
3. 更新两个 README 的安装版本。
4. 按项目 Issue/PR 流程合并上述改动。
5. 在 `main` 上运行 `python scripts/release.py check` 和 `tag`。
6. 在对应标签上运行 `python scripts/release.py publish`，或由标签工作流执行。
