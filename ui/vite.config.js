import { defineConfig } from 'vite';
import { resolve } from 'path';

// Plugin to hide Go template tags {{ ... }} during development
const goTemplatePlugin = () => ({
  name: 'go-template-plugin',
  transformIndexHtml(html, ctx) {
    if (ctx.server) { // Only run during dev server
      return html.replace(/{{.*?}}/gs, '');
    }
    return html;
  }
});

// Inject blocking <link> tags for the CSS entry points during dev so the
// browser doesn't paint an unstyled page while the JS module downloads.
// In production the extracted app-*.css is linked by Vite's HTML transform
// automatically, so this plugin only runs during the dev server.
const devCssInjectPlugin = () => ({
  name: 'dev-css-inject',
  transformIndexHtml(html, ctx) {
    if (!ctx.server) return html;
    const tags = [
      '<link rel="stylesheet" href="/src/styles.css">',
      '<link rel="stylesheet" href="/src/styles/terminal.css">',
    ].join('\n    ');
    return html.replace('</head>', `    ${tags}\n  </head>`);
  }
});

export default defineConfig(({ mode }) => ({
  root: './',
  base: mode === 'production' ? '/static/' : '/',
  plugins: [goTemplatePlugin(), devCssInjectPlugin()],
  // esbuild treats listed `console.*` calls as side-effect-free in
  // production, so the minifier drops them. We keep `console.error` and
  // `console.warn` so genuine production diagnostics still surface; only
  // the noisy debug/log/info calls scattered through socket/auth-ticker/
  // app are stripped.
  esbuild: {
    pure: mode === 'production'
      ? ['console.log', 'console.debug', 'console.info', 'console.trace']
      : [],
  },
  build: {
    outDir: 'dist',
    rollupOptions: {
      input: {
        landing:    resolve(__dirname, 'index.html'),      // public landing page
        terminal:   resolve(__dirname, 'terminal.html'),   // dashboard app shell
        uipro:      resolve(__dirname, 'ui-pro.html'),     // modern dashboard preview at /terminal-pro
        login:      resolve(__dirname, 'login.html'),
        admin:      resolve(__dirname, 'admin.html'),
        developers: resolve(__dirname, 'developers.html'), // public API reference
        swagger:    resolve(__dirname, 'swagger.html'),    // interactive OpenAPI console
        mcp:        resolve(__dirname, 'mcp.html'),        // public MCP connection guide
        settings:   resolve(__dirname, 'settings.html'),   // user self-service (account/alerts/api-keys)
      },
    },
  },
  resolve: {
    alias: {
      // Allows index.html to use /static/ while Vite maps it to /src/ during development
      '/static': resolve(__dirname, 'src')
    }
  },
  optimizeDeps: {
    include: ['apexcharts', 'htmx.org', 'html2pdf.js']
  },
  server: {
    proxy: {
      '/v1': 'http://localhost:8080',
      '/login': 'http://localhost:8080',
      '/logout': 'http://localhost:8080',
      '/upload': 'http://localhost:8080',
      '/ws': {
        target: 'ws://localhost:8080',
        ws: true
      }
    }
  }
}));
