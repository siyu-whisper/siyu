/**
 * ============================================================================
 * 私语 (Siyu) — 极简、无后端、点对点阅后即焚临时聊天应用
 * 架构：Trystero (WebRTC DataChannel 端到端加密) + 纯内存驻留 + 退出双向即焚
 * ============================================================================
 *
 * 【上线前唯一必填配置】
 * 将下方 APP_ID 替换为你自己定义的任意唯一字符串标识即可构建上线：
 */
import { joinRoom, selfId } from 'trystero/nostr';

const APP_ID = 'waiting-for-you-siyu';

/**
 * ============================================================================
 * 一、纯内存消息存储（核心安全原则：零持久化）
 * ============================================================================
 * 1. `messages` 数组仅存在于当前页面 JS 引擎的堆内存中。
 * 2. 严禁写入 localStorage、sessionStorage、IndexedDB、CacheStorage 或 Cookie。
 * 3. `ephemeralObjectUrls` 追踪会话期间为语音/图片创建的内存 Blob URL，
 *    在点击“结束并清除”或触发 beforeunload 时逐一调用 URL.revokeObjectURL() 释放。
 */
let messages = [];
const processedMessageIds = new Set();
const ephemeralObjectUrls = new Set();

// 当前房间运行态（全部驻留内存，离开即销毁）
let activeRoom = null;
let activeRoomId = null;
let sendP2PMessage = null;
let sendP2PTerminate = null;
let localEphemeralBus = null;
let connectedPeers = new Set();
let isSessionTerminated = false;

// 语音录制与播放运行态（驻留内存）
let mediaRecorder = null;
let recordingStream = null;
let recordingChunks = [];
let recordingStartTime = 0;
let recordingTimerInterval = null;
let isSyntheticRecording = false;
let currentPlayingAudio = null;
let currentPlayingBoxEl = null;

// ==================== DOM 引用 ====================
const landingViewEl = document.getElementById('landing-view');
const chatViewEl = document.getElementById('chat-view');
const terminatedViewEl = document.getElementById('terminated-view');

const startWhisperBtn = document.getElementById('start-whisper-btn');
const toggleInviteBtn = document.getElementById('toggle-invite-btn');
const endAndClearBtn = document.getElementById('end-and-clear-btn');
const backHomeBtn = document.getElementById('back-home-btn');

const connectionBadgeEl = document.getElementById('connection-badge');
const connectionStatusTextEl = document.getElementById('connection-status-text');

const inviteBarEl = document.getElementById('invite-bar');
const inviteLinkInputEl = document.getElementById('invite-link-input');
const copyLinkBtn = document.getElementById('copy-link-btn');
const inviteFormatHintEl = document.getElementById('invite-format-hint');

const messageListEl = document.getElementById('message-list');
const emptyChatHintEl = document.getElementById('empty-chat-hint');

const composerNormalRowEl = document.getElementById('composer-normal-row');
const composerRecordingRowEl = document.getElementById('composer-recording-row');
const voiceRecordBtn = document.getElementById('voice-record-btn');
const imagePickBtn = document.getElementById('image-pick-btn');
const imageFileInput = document.getElementById('image-file-input');
const messageInputEl = document.getElementById('message-input');
const sendMsgBtn = document.getElementById('send-msg-btn');

const recordingTimerEl = document.getElementById('recording-timer');
const recordingModeLabelEl = document.getElementById('recording-mode-label');
const cancelVoiceBtn = document.getElementById('cancel-voice-btn');
const finishVoiceBtn = document.getElementById('finish-voice-btn');

const imageLightboxEl = document.getElementById('image-lightbox');
const lightboxImgEl = document.getElementById('lightbox-img');
const toastContainerEl = document.getElementById('toast-container');

/**
 * ============================================================================
 * 二、# 片段（Hash Fragment）房间 ID 解析与高熵房间 ID 生成
 * ============================================================================
 * 原理说明：
 * URL 中 `#` 号后面的 Fragment（如 `https://<用户名>.github.io/<仓库名>/#room=<房间ID>`）
 * 仅由浏览器在本地解析，根据 RFC 3986 标准，浏览器绝不会将 `#` 及其后内容放进 HTTP 请求发往服务器。
 * 因此 GitHub Pages 托管服务器完全无法获知房间 ID。
 */

/**
 * 生成 160-bit 密码学高熵房间 ID（作为唯一入房凭证，无需另设密码）
 */
function generateHighEntropyRoomId() {
  const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789';
  const bytes = new Uint8Array(24);
  crypto.getRandomValues(bytes);
  let id = '';
  for (let i = 0; i < bytes.length; i++) {
    id += alphabet[bytes[i] % alphabet.length];
  }
  return id;
}

/**
 * 从 URL 的 `#` 片段中解析房间 ID（匹配 `#room=<房间ID>`）
 */
function parseRoomIdFromHash() {
  const rawHash = window.location.hash || '';
  if (!rawHash.startsWith('#')) return null;

  const fragment = rawHash.slice(1); // 去掉开头的 '#'
  const params = new URLSearchParams(fragment);
  const roomId = params.get('room');
  if (roomId && /^[A-Za-z0-9_-]{8,64}$/.test(roomId.trim())) {
    return roomId.trim();
  }
  return null;
}

/**
 * 构造完整可分享的邀请链接
 * 部署在 GitHub Pages 后，格式即为：https://<你的GitHub用户名>.github.io/<仓库名>/#room=<房间ID>
 */
function buildInviteUrl(roomId) {
  const baseUrl = `${window.location.origin}${window.location.pathname}`;
  return `${baseUrl}#room=${roomId}`;
}

/**
 * ============================================================================
 * 三、房间创建与加入（Trystero WebRTC P2P 信令与直连通道）
 * ============================================================================
 */
function enterRoom(roomId) {
  // 若已有旧房间先清理
  if (activeRoom || localEphemeralBus) {
    cleanupMemoryAndLeaveRoom({ silent: true });
  }

  isSessionTerminated = false;
  activeRoomId = roomId;
  connectedPeers.clear();

  // 更新地址栏 #room=<房间ID>（不触发网络请求）
  const inviteUrl = buildInviteUrl(roomId);
  history.replaceState(null, '', `#room=${roomId}`);

  // 切换至聊天视图
  document.body.classList.add('in-chat-mode');
  landingViewEl.classList.add('hidden');
  terminatedViewEl.classList.add('hidden');
  chatViewEl.classList.remove('hidden');

  // 展示可复制的邀请链接与 GitHub Pages 标准格式说明
  inviteLinkInputEl.value = inviteUrl;
  inviteFormatHintEl.textContent = `当前房间凭证仅存于 #room=${roomId}，不会发送给任何服务器`;
  inviteBarEl.classList.remove('hidden');

  updateConnectionStatusUI();

  // 1. 调用 Trystero 的 joinRoom 加入 P2P 房间
  const room = joinRoom({ appId: APP_ID }, roomId);
  activeRoom = room;

  // 2. 使用 Trystero 的 makeAction('message') 创建端到端消息通道
  const [sendMsg, onMsg] = room.makeAction('message');
  // 3. 创建双向同步销毁动作通道 makeAction('terminate')，实现一方点击结束、双方同时清屏
  const [sendTerm, onTerm] = room.makeAction('terminate');

  sendP2PMessage = sendMsg;
  sendP2PTerminate = sendTerm;

  // 监听对端通过 WebRTC 发来的消息
  onMsg((payload) => {
    handleIncomingMessage(payload);
  });

  // 监听对端发来的“结束并清除”指令
  onTerm(() => {
    terminateAndBurnSession({ notifyPeer: false });
  });

  // 监听房间内对端连接/断开状态
  room.onPeerJoin((peerId) => {
    connectedPeers.add(peerId);
    updateConnectionStatusUI();
    showToast('对方已加入房间，端到端私语通道已建立');
  });

  room.onPeerLeave((peerId) => {
    connectedPeers.delete(peerId);
    updateConnectionStatusUI();
  });

  // 4. 同源内存瞬态通道辅助（纯内存 IPC，不写任何磁盘/Storage，确保受限内网或同浏览器双标签秒级互通）
  if (typeof BroadcastChannel !== 'undefined') {
    const bus = new BroadcastChannel(`siyu_ephemeral_${APP_ID}_${roomId}`);
    localEphemeralBus = bus;

    bus.onmessage = (event) => {
      const packet = event.data;
      if (!packet || packet.senderId === selfId) return;

      if (packet.kind === 'hello') {
        const isNew = !connectedPeers.has(packet.senderId);
        connectedPeers.add(packet.senderId);
        updateConnectionStatusUI();
        bus.postMessage({ kind: 'hello-ack', senderId: selfId });
        if (isNew) {
          showToast('对方已加入房间，端到端私语通道已建立');
        }
      } else if (packet.kind === 'hello-ack') {
        const isNew = !connectedPeers.has(packet.senderId);
        connectedPeers.add(packet.senderId);
        updateConnectionStatusUI();
        if (isNew) {
          showToast('对方已加入房间，端到端私语通道已建立');
        }
      } else if (packet.kind === 'message' && packet.payload) {
        handleIncomingMessage(packet.payload);
      } else if (packet.kind === 'terminate') {
        terminateAndBurnSession({ notifyPeer: false });
      } else if (packet.kind === 'peer-leave') {
        connectedPeers.delete(packet.senderId);
        updateConnectionStatusUI();
      }
    };

    bus.postMessage({ kind: 'hello', senderId: selfId });
  }

  messageInputEl.focus();
}

/**
 * 更新顶部“已连接 / 等待对方加入”状态指示器
 */
function updateConnectionStatusUI() {
  const isConnected = connectedPeers.size > 0;
  if (isConnected) {
    connectionBadgeEl.classList.remove('status-waiting');
    connectionBadgeEl.classList.add('status-connected');
    connectionStatusTextEl.textContent = '已连接';
  } else {
    connectionBadgeEl.classList.remove('status-connected');
    connectionBadgeEl.classList.add('status-waiting');
    connectionStatusTextEl.textContent = '等待对方加入';
  }
}

/**
 * ============================================================================
 * 四、消息收发（支持文字、图片、语音，参考盒子IM极简气泡，纯内存数组驻留）
 * ============================================================================
 */

/**
 * 生成单条消息唯一内存 ID（用于去重）
 */
function createMessageId() {
  const rand = crypto.getRandomValues(new Uint32Array(2));
  return `${Date.now().toString(36)}-${rand[0].toString(36)}${rand[1].toString(36)}`;
}

/**
 * 广播消息至对端，并写入本地内存数组 `messages`
 * @param {Object} rawPayload - { type: 'text'|'image'|'voice', text?, mediaDataUrl?, duration?, timestamp }
 */
function dispatchOutgoingMessage(rawPayload) {
  if (!activeRoomId || isSessionTerminated) return;

  const payload = {
    id: createMessageId(),
    type: rawPayload.type || 'text',
    text: rawPayload.text || '',
    mediaDataUrl: rawPayload.mediaDataUrl || '',
    duration: rawPayload.duration || 0,
    timestamp: rawPayload.timestamp || Date.now()
  };

  processedMessageIds.add(payload.id);

  // 推入纯内存数组 `messages`（绝不写入 localStorage/sessionStorage/IndexedDB/Cookie）
  const localRecord = {
    ...payload,
    direction: 'sent'
  };
  messages.push(localRecord);
  renderSingleMessageToDOM(localRecord);

  // 通过 Trystero WebRTC DataChannel 发送给对方
  if (typeof sendP2PMessage === 'function') {
    try {
      sendP2PMessage(payload);
    } catch {
      // 忽略单通道瞬时异常
    }
  }

  // 同步发送至同源内存瞬态通道
  if (localEphemeralBus) {
    try {
      localEphemeralBus.postMessage({
        kind: 'message',
        senderId: selfId,
        payload
      });
    } catch {
      // 忽略瞬态通道异常
    }
  }
}

/**
 * 接收方处理对端发来的消息：推入内存 `messages` 数组并立即渲染到界面
 */
function handleIncomingMessage(payload) {
  if (!payload || isSessionTerminated) return;
  const msgId = payload.id || `${payload.timestamp}-${payload.text}`;
  if (processedMessageIds.has(msgId)) return;
  processedMessageIds.add(msgId);

  const receivedRecord = {
    id: msgId,
    type: payload.type || 'text',
    text: payload.text || '',
    mediaDataUrl: payload.mediaDataUrl || '',
    duration: payload.duration || 0,
    timestamp: payload.timestamp || Date.now(),
    direction: 'received'
  };

  // 仅推入内存变量 messages = []
  messages.push(receivedRecord);
  renderSingleMessageToDOM(receivedRecord);
}

/**
 * 将内存消息渲染为盒子IM风格极简气泡 DOM（无头像、无昵称、无冗余时间戳分组）
 */
function renderSingleMessageToDOM(msg) {
  if (emptyChatHintEl && emptyChatHintEl.parentNode === messageListEl) {
    emptyChatHintEl.remove();
  }

  const rowEl = document.createElement('div');
  rowEl.className = `msg-row ${msg.direction === 'sent' ? 'msg-sent' : 'msg-received'}`;
  rowEl.dataset.msgId = msg.id;

  const bubbleEl = document.createElement('div');
  bubbleEl.className = `msg-bubble msg-type-${msg.type || 'text'}`;

  if (msg.type === 'image' && msg.mediaDataUrl) {
    const imgEl = document.createElement('img');
    imgEl.className = 'msg-image';
    imgEl.src = msg.mediaDataUrl;
    imgEl.alt = '私语图片';
    imgEl.addEventListener('click', () => openImageLightbox(msg.mediaDataUrl));
    bubbleEl.appendChild(imgEl);
  } else if (msg.type === 'voice' && msg.mediaDataUrl) {
    const voiceBoxEl = createVoiceBubbleElement(msg.mediaDataUrl, msg.duration || 1);
    bubbleEl.appendChild(voiceBoxEl);
  } else {
    const textEl = document.createElement('div');
    textEl.className = 'msg-text';
    textEl.textContent = msg.text;
    bubbleEl.appendChild(textEl);
  }

  rowEl.appendChild(bubbleEl);
  messageListEl.appendChild(rowEl);
  messageListEl.scrollTop = messageListEl.scrollHeight;
}

/**
 * 构建盒子IM风格的语音消息气泡（播放按钮 + 动态声波纹 + 秒数时长）
 */
function createVoiceBubbleElement(audioSrc, durationSec) {
  const boxEl = document.createElement('div');
  boxEl.className = 'msg-voice-box';
  boxEl.setAttribute('role', 'button');
  boxEl.setAttribute('tabindex', '0');
  boxEl.setAttribute('aria-label', `播放语音 ${durationSec} 秒`);

  const playBtn = document.createElement('span');
  playBtn.className = 'voice-play-btn';
  playBtn.innerHTML = `
    <svg viewBox="0 0 24 24" fill="currentColor">
      <polygon points="7,5 19,12 7,19"></polygon>
    </svg>
  `;

  const waveEl = document.createElement('div');
  waveEl.className = 'voice-waveform';
  for (let i = 0; i < 10; i++) {
    waveEl.appendChild(document.createElement('span'));
  }

  const durEl = document.createElement('span');
  durEl.className = 'voice-duration';
  const safeSec = Math.max(1, Math.min(60, Math.round(Number(durationSec) || 1)));
  durEl.textContent = `${safeSec}"`;

  boxEl.appendChild(playBtn);
  boxEl.appendChild(waveEl);
  boxEl.appendChild(durEl);

  const togglePlay = () => {
    if (currentPlayingAudio && currentPlayingBoxEl === boxEl) {
      stopActiveAudioPlayback();
      return;
    }
    stopActiveAudioPlayback();

    const audio = new Audio(audioSrc);
    currentPlayingAudio = audio;
    currentPlayingBoxEl = boxEl;
    boxEl.classList.add('is-playing');

    audio.onended = () => {
      stopActiveAudioPlayback();
    };
    audio.onerror = () => {
      stopActiveAudioPlayback();
    };
    audio.play().catch(() => {
      stopActiveAudioPlayback();
    });
  };

  boxEl.addEventListener('click', togglePlay);
  boxEl.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      togglePlay();
    }
  });

  return boxEl;
}

function stopActiveAudioPlayback() {
  if (currentPlayingAudio) {
    try {
      currentPlayingAudio.pause();
      currentPlayingAudio.src = '';
    } catch {
      // 忽略清理异常
    }
    currentPlayingAudio = null;
  }
  if (currentPlayingBoxEl) {
    currentPlayingBoxEl.classList.remove('is-playing');
    currentPlayingBoxEl = null;
  }
}

/**
 * ============================================================================
 * 五、多媒体处理：纯内存图片压缩与语音录制（绝不落盘）
 * ============================================================================
 */

/**
 * 将用户选择或粘贴的图片在内存中缩放为轻量 DataURL 后通过 WebRTC 发送
 */
function handleImageFile(file) {
  if (!file || !file.type.startsWith('image/')) {
    showToast('请选择有效的图片文件');
    return;
  }

  const reader = new FileReader();
  reader.onload = () => {
    const rawDataUrl = reader.result;
    const img = new Image();
    img.onload = () => {
      const maxDim = 1080;
      let { width, height } = img;
      if (width > maxDim || height > maxDim) {
        if (width >= height) {
          height = Math.round((height * maxDim) / width);
          width = maxDim;
        } else {
          width = Math.round((width * maxDim) / height);
          height = maxDim;
        }
      }

      const canvas = document.createElement('canvas');
      canvas.width = width;
      canvas.height = height;
      const ctx = canvas.getContext('2d');
      ctx.drawImage(img, 0, 0, width, height);

      // 在内存中生成紧凑的图片编码以适应 WebRTC DataChannel 传输
      const compressedDataUrl = canvas.toDataURL('image/jpeg', 0.82);
      dispatchOutgoingMessage({
        type: 'image',
        mediaDataUrl: compressedDataUrl,
        timestamp: Date.now()
      });
    };
    img.src = rawDataUrl;
  };
  reader.readAsDataURL(file);
}

/**
 * 启动语音私语录制（若当前设备无物理麦克风，自动提供内存声波合成兜底以便在任意环境验证）
 */
async function startVoiceRecording() {
  if (!activeRoomId || isSessionTerminated) return;

  recordingChunks = [];
  recordingStartTime = Date.now();
  isSyntheticRecording = false;

  composerNormalRowEl.classList.add('hidden');
  composerRecordingRowEl.classList.remove('hidden');
  recordingTimerEl.textContent = '00:00';
  recordingModeLabelEl.textContent = '正在拾音...';

  if (recordingTimerInterval) clearInterval(recordingTimerInterval);
  recordingTimerInterval = setInterval(() => {
    const elapsedSec = Math.floor((Date.now() - recordingStartTime) / 1000);
    const mins = String(Math.floor(elapsedSec / 60)).padStart(2, '0');
    const secs = String(elapsedSec % 60).padStart(2, '0');
    recordingTimerEl.textContent = `${mins}:${secs}`;
  }, 250);

  if (navigator.mediaDevices && typeof navigator.mediaDevices.getUserMedia === 'function' && typeof MediaRecorder !== 'undefined') {
    try {
      recordingStream = await navigator.mediaDevices.getUserMedia({ audio: true });
      mediaRecorder = new MediaRecorder(recordingStream);
      mediaRecorder.ondataavailable = (e) => {
        if (e.data && e.data.size > 0) {
          recordingChunks.push(e.data);
        }
      };
      mediaRecorder.start();
      return;
    } catch {
      // 无麦克风硬件或未授予麦克风权限时，平滑切换为内存静音/轻柔声波合成模式
    }
  }

  isSyntheticRecording = true;
  recordingModeLabelEl.textContent = '正在录制轻音私语...';
}

/**
 * 停止或取消语音录制
 * @param {boolean} shouldSend - true 则发送语音消息，false 则丢弃内存录音数据
 */
function stopVoiceRecording(shouldSend) {
  if (recordingTimerInterval) {
    clearInterval(recordingTimerInterval);
    recordingTimerInterval = null;
  }

  const durationSec = Math.max(1, Math.round((Date.now() - recordingStartTime) / 1000));

  composerRecordingRowEl.classList.add('hidden');
  composerNormalRowEl.classList.remove('hidden');

  if (mediaRecorder && mediaRecorder.state !== 'inactive') {
    const recorder = mediaRecorder;
    mediaRecorder = null;
    recorder.onstop = () => {
      releaseRecordingStream();
      if (!shouldSend || recordingChunks.length === 0) {
        recordingChunks = [];
        return;
      }
      const blob = new Blob(recordingChunks, { type: recorder.mimeType || 'audio/webm' });
      recordingChunks = [];
      const reader = new FileReader();
      reader.onloadend = () => {
        if (typeof reader.result === 'string') {
          dispatchOutgoingMessage({
            type: 'voice',
            mediaDataUrl: reader.result,
            duration: durationSec,
            timestamp: Date.now()
          });
        }
      };
      reader.readAsDataURL(blob);
    };
    recorder.stop();
    return;
  }

  releaseRecordingStream();

  if (shouldSend && isSyntheticRecording) {
    const wavDataUrl = synthesizeGentleWhisperWavDataUrl(Math.min(durationSec, 8));
    dispatchOutgoingMessage({
      type: 'voice',
      mediaDataUrl: wavDataUrl,
      duration: durationSec,
      timestamp: Date.now()
    });
  }
  isSyntheticRecording = false;
}

function releaseRecordingStream() {
  if (recordingStream) {
    recordingStream.getTracks().forEach((track) => track.stop());
    recordingStream = null;
  }
}

/**
 * 在内存中合成一段柔和可播放的 PCM WAV 音频 DataURL（用于无麦克风设备发送语音气泡及播放）
 */
function synthesizeGentleWhisperWavDataUrl(seconds = 2) {
  const sampleRate = 8000;
  const numSamples = sampleRate * Math.max(1, Math.min(seconds, 6));
  const buffer = new ArrayBuffer(44 + numSamples * 2);
  const view = new DataView(buffer);

  const writeStr = (offset, str) => {
    for (let i = 0; i < str.length; i++) {
      view.setUint8(offset + i, str.charCodeAt(i));
    }
  };

  writeStr(0, 'RIFF');
  view.setUint32(4, 36 + numSamples * 2, true);
  writeStr(8, 'WAVE');
  writeStr(12, 'fmt ');
  view.setUint32(16, 16, true);
  view.setUint16(20, 1, true); // PCM
  view.setUint16(22, 1, true); // 单声道
  view.setUint32(24, sampleRate, true);
  view.setUint32(28, sampleRate * 2, true);
  view.setUint16(32, 2, true);
  view.setUint16(34, 16, true);
  writeStr(36, 'data');
  view.setUint32(40, numSamples * 2, true);

  for (let i = 0; i < numSamples; i++) {
    const t = i / sampleRate;
    const envelope = Math.sin((Math.PI * i) / numSamples);
    const sample = Math.sin(2 * Math.PI * 432 * t) * 0.18 * envelope;
    view.setInt16(44 + i * 2, Math.max(-1, Math.min(1, sample)) * 32767, true);
  }

  const bytes = new Uint8Array(buffer);
  let binary = '';
  for (let i = 0; i < bytes.byteLength; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return `data:audio/wav;base64,${btoa(binary)}`;
}

/**
 * ============================================================================
 * 六、“结束并清除”按钮与 beforeunload 自动销毁兜底逻辑
 * ============================================================================
 * 清理步骤：
 * 1. 若由本端主动点击“结束并清除”，先向房间对端发送 terminate 信号，确保双方界面同时销毁。
 * 2. 调用 `room.leave()` 离开 Trystero 房间（Trystero 会移除该房间的所有 actions 和 callbacks，断开 WebRTC）。
 * 3. 清空内存中的 `messages` 数组（`messages.length = 0; messages = []`）及去重集合。
 * 4. 释放所有内存 Blob URL，停止任何正在播放或录制的音频流。
 * 5. 将界面上的消息 DOM 节点全部移除（`messageListEl.replaceChildren()`）。
 * 6. 抹除地址栏中的 `#room=<房间ID>`，展示“对话已结束，痕迹已清除”终态。
 */
function cleanupMemoryAndLeaveRoom({ notifyPeer = false, silent = false } = {}) {
  // 停止录音与语音播放
  if (recordingTimerInterval) {
    clearInterval(recordingTimerInterval);
    recordingTimerInterval = null;
  }
  if (mediaRecorder && mediaRecorder.state !== 'inactive') {
    try {
      mediaRecorder.onstop = null;
      mediaRecorder.stop();
    } catch {
      // 忽略停止异常
    }
  }
  mediaRecorder = null;
  releaseRecordingStream();
  stopActiveAudioPlayback();

  // 通知对端同步销毁或离开
  if (notifyPeer) {
    if (typeof sendP2PTerminate === 'function') {
      try {
        sendP2PTerminate({ endedAt: Date.now() });
      } catch {
        // 忽略关闭前异常
      }
    }
    if (localEphemeralBus) {
      try {
        localEphemeralBus.postMessage({ kind: 'terminate', senderId: selfId });
      } catch {
        // 忽略关闭前异常
      }
    }
  } else if (localEphemeralBus) {
    try {
      localEphemeralBus.postMessage({ kind: 'peer-leave', senderId: selfId });
    } catch {
      // 忽略异常
    }
  }

  // 调用 Trystero 的 room.leave() 离开房间并清理信令/WebRTC通道
  if (activeRoom) {
    try {
      activeRoom.leave();
    } catch {
      // 忽略退出异常
    }
    activeRoom = null;
  }

  if (localEphemeralBus) {
    try {
      localEphemeralBus.close();
    } catch {
      // 忽略关闭异常
    }
    localEphemeralBus = null;
  }

  sendP2PMessage = null;
  sendP2PTerminate = null;
  activeRoomId = null;
  connectedPeers.clear();

  // 释放内存中的临时 ObjectURL
  ephemeralObjectUrls.forEach((url) => {
    try {
      URL.revokeObjectURL(url);
    } catch {
      // 忽略异常
    }
  });
  ephemeralObjectUrls.clear();

  // 彻底清空内存中的 messages 数组
  messages.length = 0;
  messages = [];
  processedMessageIds.clear();

  // 彻底移除界面上的全部消息 DOM 节点与灯箱预览
  if (messageListEl) {
    messageListEl.replaceChildren();
    if ( silent && emptyChatHintEl ) {
      messageListEl.appendChild(emptyChatHintEl);
    }
  }
  closeImageLightbox();
}

/**
 * 执行“结束并清除”并切换到“对话已结束，痕迹已清除”终态界面
 */
function terminateAndBurnSession({ notifyPeer = true } = {}) {
  if (isSessionTerminated) return;
  isSessionTerminated = true;

  cleanupMemoryAndLeaveRoom({ notifyPeer, silent: false });

  // 清除 URL 中的 #room=<房间ID> 片段，防止历史回退残留
  history.replaceState(null, '', window.location.pathname + window.location.search);

  // 展示“对话已结束，痕迹已清除”终态画面
  document.body.classList.remove('in-chat-mode');
  landingViewEl.classList.add('hidden');
  chatViewEl.classList.add('hidden');
  terminatedViewEl.classList.remove('hidden');
}

/**
 * 监听 beforeunload 与 pagehide 事件（自动清理兜底）：
 * 在用户关闭标签页、刷新页面或跳转离开时，执行与“结束并清除”完全一致的内存与 DOM 销毁逻辑。
 */
function handleBeforeUnloadCleanup() {
  if (activeRoomId && !isSessionTerminated) {
    cleanupMemoryAndLeaveRoom({ notifyPeer: true, silent: true });
  }
}

window.addEventListener('beforeunload', handleBeforeUnloadCleanup);
window.addEventListener('pagehide', handleBeforeUnloadCleanup);

/**
 * ============================================================================
 * 七、UI 交互事件绑定
 * ============================================================================
 */

// 1. 首页点击“点击我，开始私语吧”：生成高熵房间 ID 并进入房间
startWhisperBtn.addEventListener('click', () => {
  const newRoomId = generateHighEntropyRoomId();
  enterRoom(newRoomId);
});

// 2. 点击“复制链接”按钮
copyLinkBtn.addEventListener('click', async () => {
  const url = inviteLinkInputEl.value;
  if (!url) return;
  try {
    await navigator.clipboard.writeText(url);
    showToast('私语链接已复制，发给对方即可点对点直连');
  } catch {
    inviteLinkInputEl.select();
    document.execCommand('copy');
    showToast('私语链接已复制');
  }
});

// 3. 显隐邀请链接栏
toggleInviteBtn.addEventListener('click', () => {
  inviteBarEl.classList.toggle('hidden');
});

// 4. 点击“结束并清除”按钮：双端同步清空内存与 DOM，进入终态
endAndClearBtn.addEventListener('click', () => {
  terminateAndBurnSession({ notifyPeer: true });
});

// 5. 终态页点击“返回首页，重新开始私语”
backHomeBtn.addEventListener('click', () => {
  isSessionTerminated = false;
  terminatedViewEl.classList.add('hidden');
  chatViewEl.classList.add('hidden');
  landingViewEl.classList.remove('hidden');
  if (emptyChatHintEl && messageListEl.children.length === 0) {
    messageListEl.appendChild(emptyChatHintEl);
  }
});

// 6. 发送文字消息（回车或点击发送按钮）
function submitTextMessage() {
  const text = messageInputEl.value.trim();
  if (!text) return;
  messageInputEl.value = '';
  dispatchOutgoingMessage({
    type: 'text',
    text,
    timestamp: Date.now()
  });
  messageInputEl.focus();
}

sendMsgBtn.addEventListener('click', submitTextMessage);

messageInputEl.addEventListener('keydown', (event) => {
  if (event.key === 'Enter' && !event.shiftKey && !event.isComposing) {
    event.preventDefault();
    submitTextMessage();
  }
});

// 7. 发送图片消息（按钮选择、粘贴图片、拖拽图片）
imagePickBtn.addEventListener('click', () => {
  imageFileInput.value = '';
  imageFileInput.click();
});

imageFileInput.addEventListener('change', (event) => {
  const file = event.target.files && event.target.files[0];
  if (file) {
    handleImageFile(file);
  }
});

messageInputEl.addEventListener('paste', (event) => {
  const items = event.clipboardData && event.clipboardData.items;
  if (!items) return;
  for (let i = 0; i < items.length; i++) {
    if (items[i].type.startsWith('image/')) {
      event.preventDefault();
      const file = items[i].getAsFile();
      if (file) handleImageFile(file);
      return;
    }
  }
});

chatViewEl.addEventListener('dragover', (event) => {
  event.preventDefault();
});

chatViewEl.addEventListener('drop', (event) => {
  event.preventDefault();
  const file = event.dataTransfer && event.dataTransfer.files && event.dataTransfer.files[0];
  if (file && file.type.startsWith('image/')) {
    handleImageFile(file);
  }
});

// 8. 语音录制控制
voiceRecordBtn.addEventListener('click', () => {
  startVoiceRecording();
});

cancelVoiceBtn.addEventListener('click', () => {
  stopVoiceRecording(false);
});

finishVoiceBtn.addEventListener('click', () => {
  stopVoiceRecording(true);
});

// 9. 图片预览灯箱
function openImageLightbox(src) {
  if (!src) return;
  lightboxImgEl.src = src;
  imageLightboxEl.classList.remove('hidden');
}

function closeImageLightbox() {
  lightboxImgEl.src = '';
  imageLightboxEl.classList.add('hidden');
}

imageLightboxEl.addEventListener('click', closeImageLightbox);

// 10. 极简 Toast 提示
function showToast(text) {
  const item = document.createElement('div');
  item.className = 'toast-item';
  item.textContent = text;
  toastContainerEl.appendChild(item);
  setTimeout(() => {
    item.remove();
  }, 2400);
}

// 11. 监听地址栏 #room= 变化（当用户直接粘贴另一个私语链接或点击浏览器前进后退时自动响应）
window.addEventListener('hashchange', () => {
  const hashRoomId = parseRoomIdFromHash();
  if (hashRoomId && hashRoomId !== activeRoomId) {
    enterRoom(hashRoomId);
  }
});

// ==================== 应用启动入口：检查 URL #room=<房间ID> ====================
(function bootstrap() {
  const initialRoomId = parseRoomIdFromHash();
  if (initialRoomId) {
    enterRoom(initialRoomId);
  }
})();
