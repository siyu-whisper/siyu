import { defineConfig } from 'vite';

/**
 * GitHub Pages 部署配置说明：
 * 1. 如果你的仓库部署在 https://<你的GitHub用户名>.github.io/<仓库名>/，
 *    可将 base 设置为 '/<仓库名>/'（例如 base: '/siyu/'），
 *    或者保持默认的相对路径 './'（对根域名和任意子路径 '/<仓库名>/' 均自动兼容，不会出现静态资源 404）。
 * 2. 构建命令：npm run build，产物输出在 dist/ 目录，直接推送到 gh-pages 分支即可。
 */
export default defineConfig({
  base: process.env.VITE_BASE_PATH || './',
  plugins: [
    {
      name: 'strip-dev-importmap-on-build',
      apply: 'build',
      transformIndexHtml(html) {
        return html.replace(/<script type="importmap">[\s\S]*?<\/script>\s*/i, '');
      }
    }
  ],
  build: {
    outDir: 'dist',
    assetsDir: 'assets',
    sourcemap: false,
    target: 'es2020'
  }
});
