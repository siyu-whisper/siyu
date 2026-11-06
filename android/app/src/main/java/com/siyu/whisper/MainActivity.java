package com.siyu.whisper;

import android.Manifest;
import android.app.Activity;
import android.content.ClipData;
import android.content.ClipboardManager;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.graphics.Color;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.view.View;
import android.view.Window;
import android.view.WindowManager;
import android.webkit.CookieManager;
import android.webkit.JavascriptInterface;
import android.webkit.PermissionRequest;
import android.webkit.ValueCallback;
import android.webkit.WebChromeClient;
import android.webkit.WebResourceRequest;
import android.webkit.WebResourceResponse;
import android.webkit.WebSettings;
import android.webkit.WebStorage;
import android.webkit.WebView;
import android.webkit.WebViewClient;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 私语 (Siyu) 安卓原生客户端宿主容器
 * - 通过 https://siyu.app.local/ 安全上下文托管本地打包的 Web 资源，100% 启用 WebCrypto AES-GCM-256 与 WebRTC
 * - 与网页版 (https://siyu-whisper.github.io/siyu/) 共用完全相同的业务逻辑、房间信令、长椅暗语与风铃提醒
 * - 自动桥接安卓麦克风/摄像头动态权限 (WebRTC 语音录制、实时语音、1080P高清视频通话) 与相册选图
 * - 退出时自动清空内存、WebStorage 与缓存，真正做到阅后即焚不留痕
 */
public class MainActivity extends Activity {

    private static final String LOCAL_HTTPS_HOST = "siyu.app.local";
    private static final String BASE_ENTRY_URL = "https://siyu.app.local/index.html";
    private static final int REQ_WEBRTC_PERMISSIONS = 1001;
    private static final int REQ_FILE_CHOOSER = 1002;

    private WebView webView;
    private PermissionRequest pendingWebPermissionRequest;
    private ValueCallback<Uri[]> pendingFilePathCallback;

    public static class SiyuJsBridge {
        private final Context context;

        public SiyuJsBridge(Context context) {
            this.context = context;
        }

        @JavascriptInterface
        public boolean isAndroidApp() {
            return true;
        }

        @JavascriptInterface
        public void copyText(String text) {
            if (text == null) return;
            try {
                ClipboardManager cm = (ClipboardManager) context.getSystemService(Context.CLIPBOARD_SERVICE);
                if (cm != null) {
                    cm.setPrimaryClip(ClipData.newPlainText("siyu-invite", text));
                }
            } catch (Exception ignored) {
            }
        }
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        configureSystemBars();

        webView = new WebView(this);
        webView.setBackgroundColor(Color.parseColor("#E9E8EF"));
        setContentView(webView);

        configureWebViewSettings(webView.getSettings());
        webView.addJavascriptInterface(new SiyuJsBridge(this), "SiyuAndroidBridge");

        webView.setWebViewClient(new WebViewClient() {
            @Override
            public WebResourceResponse shouldInterceptRequest(WebView view, WebResourceRequest request) {
                if (request == null || request.getUrl() == null) {
                    return null;
                }
                Uri uri = request.getUrl();
                if (!LOCAL_HTTPS_HOST.equalsIgnoreCase(uri.getHost())) {
                    return null;
                }
                return serveLocalAsset(uri.getPath());
            }

            @Override
            public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest request) {
                if (request == null || request.getUrl() == null) {
                    return false;
                }
                Uri uri = request.getUrl();
                String host = uri.getHost() != null ? uri.getHost() : "";
                if (LOCAL_HTTPS_HOST.equalsIgnoreCase(host)) {
                    return false;
                }
                String fullUrl = uri.toString();
                if (fullUrl.contains("room=")) {
                    dispatchDeepLinkToWeb(fullUrl);
                    return true;
                }
                try {
                    Intent browserIntent = new Intent(Intent.ACTION_VIEW, uri);
                    startActivity(browserIntent);
                } catch (Exception ignored) {
                }
                return true;
            }
        });

        webView.setWebChromeClient(new WebChromeClient() {
            @Override
            public void onPermissionRequest(final PermissionRequest request) {
                runOnUiThread(new Runnable() {
                    @Override
                    public void run() {
                        handleWebRtcPermissionRequest(request);
                    }
                });
            }

            @Override
            public boolean onShowFileChooser(
                    WebView view,
                    ValueCallback<Uri[]> filePathCallback,
                    FileChooserParams fileChooserParams
            ) {
                if (pendingFilePathCallback != null) {
                    pendingFilePathCallback.onReceiveValue(null);
                    pendingFilePathCallback = null;
                }
                pendingFilePathCallback = filePathCallback;

                Intent contentSelectionIntent = new Intent(Intent.ACTION_GET_CONTENT);
                contentSelectionIntent.addCategory(Intent.CATEGORY_OPENABLE);
                contentSelectionIntent.setType("*/*");
                contentSelectionIntent.putExtra(Intent.EXTRA_MIME_TYPES, new String[]{"image/*", "video/*"});

                try {
                    startActivityForResult(
                            Intent.createChooser(contentSelectionIntent, "选择要发送的图片或视频"),
                            REQ_FILE_CHOOSER
                    );
                    return true;
                } catch (Exception e) {
                    pendingFilePathCallback = null;
                    return false;
                }
            }
        });

        String initialUrl = buildEntryUrlFromIntent(getIntent());
        webView.loadUrl(initialUrl);
    }

    @Override
    protected void onNewIntent(Intent intent) {
        super.onNewIntent(intent);
        setIntent(intent);
        if (intent != null && intent.getData() != null) {
            dispatchDeepLinkToWeb(intent.getData().toString());
        }
    }

    private void configureSystemBars() {
        try {
            Window window = getWindow();
            if (window == null) return;
            window.addFlags(WindowManager.LayoutParams.FLAG_DRAWS_SYSTEM_BAR_BACKGROUNDS);
            int mistColor = Color.parseColor("#E9E8EF");
            window.setStatusBarColor(mistColor);
            window.setNavigationBarColor(mistColor);
            View decor = window.getDecorView();
            int flags = decor.getSystemUiVisibility();
            flags |= View.SYSTEM_UI_FLAG_LIGHT_STATUS_BAR;
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                flags |= View.SYSTEM_UI_FLAG_LIGHT_NAVIGATION_BAR;
            }
            decor.setSystemUiVisibility(flags);
        } catch (Exception ignored) {
        }
    }

    private void configureWebViewSettings(WebSettings settings) {
        settings.setJavaScriptEnabled(true);
        settings.setDomStorageEnabled(true);
        settings.setDatabaseEnabled(false);
        settings.setMediaPlaybackRequiresUserGesture(false);
        settings.setAllowFileAccess(true);
        settings.setAllowContentAccess(true);
        settings.setCacheMode(WebSettings.LOAD_NO_CACHE);
        settings.setSupportZoom(false);
        settings.setBuiltInZoomControls(false);
        settings.setDisplayZoomControls(false);
        settings.setMixedContentMode(WebSettings.MIXED_CONTENT_ALWAYS_ALLOW);
    }

    private WebResourceResponse serveLocalAsset(String rawPath) {
        String cleanPath = (rawPath == null || rawPath.isEmpty() || "/".equals(rawPath))
                ? "index.html"
                : (rawPath.startsWith("/") ? rawPath.substring(1) : rawPath);

        String assetPath = "www/" + cleanPath;
        Map<String, String> headers = new HashMap<>();
        headers.put("Access-Control-Allow-Origin", "*");
        headers.put("Cache-Control", "no-store, no-cache, must-revalidate");

        try {
            InputStream stream = getAssets().open(assetPath);
            String mimeType = resolveMimeType(cleanPath);
            return new WebResourceResponse(mimeType, "UTF-8", 200, "OK", headers, stream);
        } catch (Exception e) {
            byte[] empty = "Not Found".getBytes(StandardCharsets.UTF_8);
            return new WebResourceResponse(
                    "text/plain",
                    "UTF-8",
                    404,
                    "Not Found",
                    headers,
                    new ByteArrayInputStream(empty)
            );
        }
    }

    private String resolveMimeType(String path) {
        String lower = path.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".html") || lower.endsWith(".htm")) return "text/html";
        if (lower.endsWith(".js") || lower.endsWith(".mjs")) return "text/javascript";
        if (lower.endsWith(".css")) return "text/css";
        if (lower.endsWith(".svg")) return "image/svg+xml";
        if (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) return "image/jpeg";
        if (lower.endsWith(".png")) return "image/png";
        if (lower.endsWith(".webp")) return "image/webp";
        if (lower.endsWith(".json") || lower.endsWith(".webmanifest")) return "application/json";
        if (lower.endsWith(".woff2")) return "font/woff2";
        return "application/octet-stream";
    }

    private String buildEntryUrlFromIntent(Intent intent) {
        if (intent != null && intent.getData() != null) {
            Uri data = intent.getData();
            String fragment = data.getFragment();
            if (fragment != null && fragment.contains("room=")) {
                return BASE_ENTRY_URL + "#" + fragment;
            }
            String queryRoom = data.getQueryParameter("room");
            if (queryRoom != null && !queryRoom.trim().isEmpty()) {
                return BASE_ENTRY_URL + "#room=" + queryRoom.trim();
            }
        }
        return BASE_ENTRY_URL;
    }

    private void dispatchDeepLinkToWeb(String rawUrl) {
        if (webView == null || rawUrl == null) return;
        String escaped = rawUrl.replace("\\", "\\\\").replace("'", "\\'");
        webView.evaluateJavascript(
                "(function(){ if(window.handleAndroidDeepLink){ window.handleAndroidDeepLink('" + escaped + "'); } })();",
                null
        );
    }

    private void handleWebRtcPermissionRequest(PermissionRequest request) {
        String[] resources = request.getResources();
        List<String> missingOsPerms = new ArrayList<>();

        for (String res : resources) {
            if (PermissionRequest.RESOURCE_AUDIO_CAPTURE.equals(res)) {
                if (checkSelfPermission(Manifest.permission.RECORD_AUDIO) != PackageManager.PERMISSION_GRANTED) {
                    missingOsPerms.add(Manifest.permission.RECORD_AUDIO);
                }
            } else if (PermissionRequest.RESOURCE_VIDEO_CAPTURE.equals(res)) {
                if (checkSelfPermission(Manifest.permission.CAMERA) != PackageManager.PERMISSION_GRANTED) {
                    missingOsPerms.add(Manifest.permission.CAMERA);
                }
            }
        }

        if (missingOsPerms.isEmpty()) {
            request.grant(resources);
        } else {
            pendingWebPermissionRequest = request;
            requestPermissions(missingOsPerms.toArray(new String[0]), REQ_WEBRTC_PERMISSIONS);
        }
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, String[] permissions, int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode == REQ_WEBRTC_PERMISSIONS && pendingWebPermissionRequest != null) {
            PermissionRequest req = pendingWebPermissionRequest;
            pendingWebPermissionRequest = null;
            List<String> grantedWebResources = new ArrayList<>();
            for (String res : req.getResources()) {
                if (PermissionRequest.RESOURCE_AUDIO_CAPTURE.equals(res)
                        && checkSelfPermission(Manifest.permission.RECORD_AUDIO) == PackageManager.PERMISSION_GRANTED) {
                    grantedWebResources.add(res);
                } else if (PermissionRequest.RESOURCE_VIDEO_CAPTURE.equals(res)
                        && checkSelfPermission(Manifest.permission.CAMERA) == PackageManager.PERMISSION_GRANTED) {
                    grantedWebResources.add(res);
                }
            }
            if (!grantedWebResources.isEmpty()) {
                req.grant(grantedWebResources.toArray(new String[0]));
            } else {
                req.deny();
            }
        }
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (requestCode == REQ_FILE_CHOOSER) {
            if (pendingFilePathCallback == null) return;
            Uri[] results = null;
            if (resultCode == Activity.RESULT_OK && data != null) {
                if (data.getData() != null) {
                    results = new Uri[]{data.getData()};
                } else if (data.getClipData() != null && data.getClipData().getItemCount() > 0) {
                    results = new Uri[]{data.getClipData().getItemAt(0).getUri()};
                }
            }
            pendingFilePathCallback.onReceiveValue(results);
            pendingFilePathCallback = null;
        }
    }

    @Override
    public void onBackPressed() {
        if (webView == null) {
            super.onBackPressed();
            return;
        }
        webView.evaluateJavascript(
                "(typeof window.handleAndroidBackPress === 'function') ? Boolean(window.handleAndroidBackPress()) : false",
                new ValueCallback<String>() {
                    @Override
                    public void onReceiveValue(String value) {
                        if (!"true".equals(value)) {
                            MainActivity.super.onBackPressed();
                        }
                    }
                }
        );
    }

    @Override
    protected void onDestroy() {
        try {
            if (webView != null) {
                webView.loadUrl("about:blank");
                webView.clearHistory();
                webView.clearFormData();
                webView.clearCache(true);
                webView.removeAllViews();
                webView.destroy();
                webView = null;
            }
            WebStorage.getInstance().deleteAllData();
            CookieManager.getInstance().removeAllCookies(null);
            CookieManager.getInstance().flush();
        } catch (Exception ignored) {
        }
        super.onDestroy();
    }
}
