# 私语 (Siyu) — 极简、无后端、点对点临时私密聊天

> **在线体验（GitHub Pages）**：[https://siyu-whisper.github.io/siyu/](https://siyu-whisper.github.io/siyu/)
>
> **核心原则**：消息不经过任何应用服务器，不写入任何持久化存储（零 `localStorage` / `sessionStorage` / `IndexedDB` / `Cookie`），关闭页面或点击“结束并清除”后双方数据同时从内存中清除，房间链接随即失效。

## 目录结构

```text
siyu/
├── index.html                   # 页面入口（首页、聊天视图、终态销毁视图）
├── main.js                      # 核心 P2P 逻辑（Trystero + 内存数组 + 多媒体收发 + 双向销毁）
├── style.css                    # 极简雪原迷雾意境与盒子IM风格气泡样式
├── package.json                 # 依赖与构建脚本（trystero + vite，MIT 协议）
├── vite.config.js               # Vite 构建与 GitHub Pages base 路径配置
├── LICENSE                      # MIT 开源许可证
├── .gitignore                   # Git 忽略规则（忽略 node_modules/ 与 dist/）
├── public/
│   ├── logo.svg                 # 私语专属极简小 Logo（双人秋千与耳语声波）
│   └── snow-swing-bg.svg        # 苍茫雪原与浓雾中的双人秋千背景矢量画卷
└── .github/workflows/deploy.yml # GitHub Actions 自动构建并推送到 gh-pages 分支
```

## 快速上线步骤（GitHub Pages）

1. **确认专属 `APP_ID`**：
   `main.js` 顶部的 `APP_ID` 已配置为：

   ```js
   const APP_ID = 'waiting-for-you-siyu';
   ```

2. **安装依赖与构建**：

   ```bash
   npm install
   npm run build
   ```

   > 若你的 GitHub Pages 地址是 `https://<你的GitHub用户名>.github.io/<仓库名>/`，`vite.config.js` 默认采用相对路径 `'./'`，也可显式设置 `base: '/<仓库名>/'`。

3. **推送到 GitHub Pages**：
   - **方式 A（手动推送 `dist` 到 `gh-pages` 分支）**：

     ```bash
     npx gh-pages -d dist
     ```

   - **方式 B（自动部署）**：推送到 `main` 分支后，内置的 `.github/workflows/deploy.yml` 会自动构建并将产物发布到 `gh-pages` 分支。
   - 在 GitHub 仓库 **Settings → Pages → Build and deployment** 中，选择 `Deploy from a branch` → `gh-pages` 分支（`/(root)`）即可上线。

## 一键推送至新 GitHub 仓库命令

```bash
cd output/siyu
git init
git add .
git commit -m "feat: initial release of Siyu ephemeral P2P chat"
git branch -M main
git remote add origin https://github.com/siyu-whisper/siyu.git
git push -u origin main
```

## 开源协议与致谢

- 本项目基于 [MIT License](./LICENSE) 开源，支持任何人自由 Fork 与自托管部署。
- P2P WebRTC 信令抽象层基于 [Trystero](https://github.com/dmotz/trystero)（MIT License）。
- 静态无后端自托管模式受 [Chitchatter](https://github.com/jeremyckahn/chitchatter) 启发，全部源码与 SVG 视觉资产均为独立原创编写。
