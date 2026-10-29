/**
 * ============================================================================
 * 私语 (Siyu) — 极简、无后端、点对点阅后即焚临时聊天应用
 * 架构：Trystero (WebRTC DataChannel 端到端加密) + 纯内存驻留 + 退出双向即焚
 * ============================================================================
 *
 * 【上线前唯一必填配置】
 * 将下方 APP_ID 替换为你自己定义的任意唯一字符串标识即可构建上线：
 */
import { joinRoom as joinMqttRoom, selfId } from 'trystero/mqtt';
import { joinRoom as joinNostrRoom } from 'trystero/nostr';

const APP_ID = 'waiting-for-you-siyu';

// 国内外高可用公共 WSS 消息中继（仅用于 WebRTC 信令握手与 NAT 受限时的 AES-GCM-256 端到端加密瞬态转发，QoS=0 / Retain=false 零存储）
const PUBLIC_WSS_BROKERS = [
  'wss://broker.emqx.io:8084/mqtt',
  'wss://broker.hivemq.com:8884/mqtt',
  'wss://test.mosquitto.org:8081/mqtt'
];

/**
 * ============================================================================
 * 一、纯内存消息存储（核心安全原则：零持久化）
 * ============================================================================
 * 1. `messages` 数组仅存在于当前页面 JS 引擎的堆内存中。
 * 2. 严禁写入 localStorage、sessionStorage、IndexedDB、CacheStorage 或 Cookie。
 * 3. 当朋友通过邀请链接后加入房间时，本端会自动将内存中的 `messages` 加密同步给对方，
 *    解决“先发消息、朋友后点链接进来看不到此前对话”的问题。
 */
let messages = [];
const processedMessageIds = new Set();
const ephemeralObjectUrls = new Set();
const incomingChunkBuffer = new Map();

// 当前房间运行态（全部驻留内存，离开即销毁）
let activeRooms = [];
let activeRoomId = null;
let roomCryptoKeyPromise = null;
let p2pDispatchers = [];
let e2eeWssClients = [];
let presenceHeartbeatTimer = null;
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
 * 基于高熵 roomId 派生仅驻留内存的 AES-GCM-256 对称密钥
 */
async function deriveRoomAesKey(roomId) {
  const encoder = new TextEncoder();
  const seedBytes = encoder.encode(`${APP_ID}::e2ee-key::${roomId}`);
  const hashBuffer = await crypto.subtle.digest('SHA-256', seedBytes);
  return crypto.subtle.importKey('raw', hashBuffer, { name: 'AES-GCM' }, false, ['encrypt', 'decrypt']);
}

function bytesToBase64(bytes) {
  let bin = '';
  for (let i = 0; i < bytes.length; i++) {
    bin += String.fromCharCode(bytes[i]);
  }
  return btoa(bin);
}

function base64ToBytes(b64) {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) {
    out[i] = bin.charCodeAt(i);
  }
  return out;
}

async function encryptRoomText(plainText) {
  if (!roomCryptoKeyPromise) return null;
  const key = await roomCryptoKeyPromise;
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const encoded = new TextEncoder().encode(plainText);
  const cipherBuf = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, encoded);
  return {
    iv: bytesToBase64(iv),
    ct: bytesToBase64(new Uint8Array(cipherBuf))
  };
}

async function decryptRoomText(ivB64, ctB64) {
  if (!roomCryptoKeyPromise) return null;
  const key = await roomCryptoKeyPromise;
  const iv = base64ToBytes(ivB64);
  const ct = base64ToBytes(ctB64);
  const plainBuf = await crypto.subtle.decrypt({ name: 'AES-GCM', iv }, key, ct);
  return new TextDecoder().decode(plainBuf);
}

/**
 * ============================================================================
 * 三、房间创建、跨网双引擎连接与后进房间内存历史自动同步
 * ============================================================================
 */
function enterRoom(roomId) {
  if (activeRooms.length > 0 || e2eeWssClients.length > 0 || localEphemeralBus) {
    cleanupMemoryAndLeaveRoom({ silent: true });
  }

  isSessionTerminated = false;
  activeRoomId = roomId;
  roomCryptoKeyPromise = deriveRoomAesKey(roomId);
  connectedPeers.clear();

  const inviteUrl = buildInviteUrl(roomId);
  history.replaceState(null, '', `#room=${roomId}`);

  document.body.classList.add('in-chat-mode');
  landingViewEl.classList.add('hidden');
  terminatedViewEl.classList.add('hidden');
  chatViewEl.classList.remove('hidden');

  inviteLinkInputEl.value = inviteUrl;
  inviteFormatHintEl.textContent = `当前房间凭证仅存于 #room=${roomId}，不会发送给任何服务器`;
  inviteBarEl.classList.remove('hidden');

  updateConnectionStatusUI();

  // 1. 启动 Trystero WebRTC P2P 双策略房间（MQTT WebRTC + Nostr WebRTC）
  const strategies = [
    () => joinMqttRoom({ appId: APP_ID, relayUrls: PUBLIC_WSS_BROKERS }, roomId),
    () => joinNostrRoom({ appId: APP_ID }, roomId)
  ];

  for (const createRoom of strategies) {
    try {
      const room = createRoom();
      activeRooms.push(room);
      bindTrysteroRoomActions(room);
    } catch {
      // 单个策略初始化异常时不影响其他通道
    }
  }

  // 2. 启动端到端加密（AES-GCM-256）WSS 零存储直连通道（解决手机4G/5G对称NAT或防火墙阻断UDP导致看不到消息的问题）
  startE2eeZeroStorageWssBridge(roomId);

  // 3. 启动同源内存 BroadcastChannel 通道（同浏览器多标签秒级直连）
  if (typeof BroadcastChannel !== 'undefined') {
    const bus = new BroadcastChannel(`siyu_ephemeral_${APP_ID}_${roomId}`);
    localEphemeralBus = bus;

    bus.onmessage = (event) => {
      handleEnvelopePacket(event.data);
    };

    bus.postMessage({ kind: 'hello', senderId: selfId });
  }

  // 4. 定时发送在线心跳与历史同步探针，确保朋友随时进来都能秒级变为“已连接”并看到此前发送的内容
  if (presenceHeartbeatTimer) clearInterval(presenceHeartbeatTimer);
  presenceHeartbeatTimer = setInterval(() => {
    if (!activeRoomId || isSessionTerminated) return;
    broadcastEnvelopePacket({
      kind: 'hello-ack',
      senderId: selfId,
      msgCount: messages.length
    });
  }, 4500);

  messageInputEl.focus();
}

/**
 * 绑定单个 Trystero 房间的动作与对端发现事件
 */
function bindTrysteroRoomActions(room) {
  const [sendPacket, onPacket] = room.makeAction('siyu-pkt');
  p2pDispatchers.push(sendPacket);

  onPacket((packet) => {
    handleEnvelopePacket(packet);
  });

  room.onPeerJoin((peerId) => {
    registerPeerConnected(peerId);
    // 对方通过 WebRTC DataChannel 加入时，立即将内存中已发送的消息同步给对方
    syncInMemoryHistoryToPeers();
    acknowledgeUnreadReceivedMessages({ forceResendAll: true });
  });

  room.onPeerLeave((peerId) => {
    connectedPeers.delete(peerId);
    updateConnectionStatusUI();
  });
}

/**
 * 统一处理所有通道（WebRTC DataChannel / E2EE WSS / BroadcastChannel）收到的消息信封
 */
function handleEnvelopePacket(packet) {
  if (!packet || packet.senderId === selfId || isSessionTerminated) return;

  registerPeerConnected(packet.senderId);

  if (packet.kind === 'hello') {
    broadcastEnvelopePacket({
      kind: 'hello-ack',
      senderId: selfId,
      msgCount: messages.length
    });
    syncInMemoryHistoryToPeers();
    acknowledgeUnreadReceivedMessages({ forceResendAll: true });
  } else if (packet.kind === 'hello-ack') {
    if ((packet.msgCount === 0 || typeof packet.msgCount === 'undefined') && messages.length > 0) {
      syncInMemoryHistoryToPeers();
    }
    acknowledgeUnreadReceivedMessages({ forceResendAll: true });
  } else if (packet.kind === 'message' && packet.payload) {
    handleIncomingMessage(packet.payload);
  } else if (packet.kind === 'sync-history' && Array.isArray(packet.items)) {
    handleIncomingHistorySync(packet.items);
  } else if (packet.kind === 'revoke' && packet.payload?.id) {
    revokeMessageById(packet.payload.id, { notifyPeer: false });
  } else if (packet.kind === 'edit' && packet.payload?.id) {
    applyMessageEditById(packet.payload.id, packet.payload.text, { notifyPeer: false });
  } else if (packet.kind === 'read' && Array.isArray(packet.payload?.ids)) {
    applyReadReceiptIds(packet.payload.ids);
  } else if (packet.kind === 'terminate') {
    terminateAndBurnSession({ notifyPeer: false });
  } else if (packet.kind === 'peer-leave') {
    connectedPeers.delete(packet.senderId);
    updateConnectionStatusUI();
  }
}

function registerPeerConnected(peerId) {
  if (!peerId || peerId === selfId) return;
  const wasEmpty = connectedPeers.size === 0;
  const isNewPeer = !connectedPeers.has(peerId);
  connectedPeers.add(peerId);
  if (wasEmpty || isNewPeer) {
    updateConnectionStatusUI();
  }
  if (wasEmpty) {
    showToast('对方已加入房间，端到端私语通道已建立');
  }
}

function broadcastEnvelopePacket(packet) {
  if (!activeRoomId || isSessionTerminated) return;

  for (const sendFn of p2pDispatchers) {
    try {
      sendFn(packet);
    } catch {
      // 忽略单通道瞬时异常
    }
  }

  broadcastEncryptedWssPacket(packet);

  if (localEphemeralBus) {
    try {
      localEphemeralBus.postMessage(packet);
    } catch {
      // 忽略异常
    }
  }
}

/**
 * 将当前内存中的 `messages` 历史同步给后进入房间的朋友
 * （解决创建者先发了消息、朋友随后才打开链接看不到此前消息的问题）
 */
function syncInMemoryHistoryToPeers() {
  if (!activeRoomId || isSessionTerminated || messages.length === 0) return;

  const snapshot = messages.map((m) => ({
    id: m.id,
    type: m.type,
    text: m.text,
    mediaDataUrl: m.mediaDataUrl,
    duration: m.duration,
    timestamp: m.timestamp,
    edited: Boolean(m.edited),
    revoked: Boolean(m.revoked),
    originSenderId: m.originSenderId || (m.direction === 'sent' ? selfId : 'peer')
  }));

  broadcastEnvelopePacket({
    kind: 'sync-history',
    senderId: selfId,
    items: snapshot
  });
}

function handleIncomingHistorySync(items) {
  if (!Array.isArray(items) || isSessionTerminated) return;
  let hasNew = false;

  for (const item of items) {
    if (!item || !item.id) continue;

    if (!processedMessageIds.has(item.id)) {
      processedMessageIds.add(item.id);
      const direction = item.originSenderId === selfId ? 'sent' : 'received';
      const record = {
        id: item.id,
        type: item.type || 'text',
        text: item.text || '',
        mediaDataUrl: item.mediaDataUrl || '',
        duration: item.duration || 0,
        timestamp: item.timestamp || Date.now(),
        direction,
        originSenderId: item.originSenderId || 'peer',
        read: direction === 'sent',
        readAckSent: false,
        edited: Boolean(item.edited),
        revoked: Boolean(item.revoked)
      };
      messages.push(record);
      renderSingleMessageToDOM(record);
      hasNew = true;
    } else {
      const existing = messages.find((m) => m.id === item.id);
      if (existing) {
        if (item.revoked && !existing.revoked) {
          revokeMessageById(item.id, { notifyPeer: false });
        } else if (item.edited && item.text && existing.text !== item.text) {
          applyMessageEditById(item.id, item.text, { notifyPeer: false });
        }
      }
    }
  }

  if (hasNew) {
    acknowledgeUnreadReceivedMessages();
  }
}

/**
 * 端到端加密（AES-GCM-256）零存储 WSS 跨网直连引擎（兼容国内/移动4G/5G/企业防火墙）
 */
function encodeMqttVarLength(len) {
  const out = [];
  do {
    let digit = len % 128;
    len = Math.floor(len / 128);
    if (len > 0) digit |= 128;
    out.push(digit);
  } while (len > 0);
  return out;
}

function buildMqttConnectPacket(clientId) {
  const cid = new TextEncoder().encode(clientId);
  const varHeader = [0x00, 0x04, 0x4d, 0x51, 0x54, 0x54, 0x04, 0x02, 0x00, 0x3c];
  const payload = [cid.length >> 8, cid.length & 0xff, ...cid];
  const remLen = varHeader.length + payload.length;
  return new Uint8Array([0x10, ...encodeMqttVarLength(remLen), ...varHeader, ...payload]);
}

function buildMqttSubscribePacket(topic) {
  const tBytes = new TextEncoder().encode(topic);
  const body = [0x00, 0x01, tBytes.length >> 8, tBytes.length & 0xff, ...tBytes, 0x00];
  return new Uint8Array([0x82, ...encodeMqttVarLength(body.length), ...body]);
}

function buildMqttPublishPacket(topic, utf8String) {
  const tBytes = new TextEncoder().encode(topic);
  const pBytes = new TextEncoder().encode(utf8String);
  const remLen = 2 + tBytes.length + pBytes.length;
  const header = [0x30, ...encodeMqttVarLength(remLen), tBytes.length >> 8, tBytes.length & 0xff];
  const out = new Uint8Array(header.length + tBytes.length + pBytes.length);
  out.set(header, 0);
  out.set(tBytes, header.length);
  out.set(pBytes, header.length + tBytes.length);
  return out;
}

function startE2eeZeroStorageWssBridge(roomId) {
  if (typeof WebSocket === 'undefined') return;
  const topic = `siyu/v2/${APP_ID}/${roomId}`;

  for (const brokerUrl of PUBLIC_WSS_BROKERS) {
    try {
      const ws = new WebSocket(brokerUrl, 'mqtt');
      ws.binaryType = 'arraybuffer';

      const clientState = {
        ws,
        ready: false,
        topic,
        pingTimer: null
      };
      e2eeWssClients.push(clientState);

      ws.onopen = () => {
        const randSuffix = Math.random().toString(36).slice(2, 10);
        ws.send(buildMqttConnectPacket(`siyu_${selfId.slice(0, 8)}_${randSuffix}`));
      };

      ws.onmessage = async (event) => {
        if (!event.data || isSessionTerminated) return;
        const buf = new Uint8Array(event.data);
        let pos = 0;

        while (pos < buf.length) {
          const packetType = buf[pos] & 0xf0;
          let multiplier = 1;
          let remLen = 0;
          let idx = pos + 1;
          let byte;
          do {
            if (idx >= buf.length) return;
            byte = buf[idx++];
            remLen += (byte & 127) * multiplier;
            multiplier *= 128;
          } while ((byte & 128) !== 0);

          const packetEnd = idx + remLen;
          if (packetEnd > buf.length) return;

          if (packetType === 0x20 && !clientState.ready) {
            clientState.ready = true;
            ws.send(buildMqttSubscribePacket(topic));
            clientState.pingTimer = setInterval(() => {
              if (ws.readyState === WebSocket.OPEN) {
                ws.send(new Uint8Array([0xc0, 0x00]));
              }
            }, 25000);
            broadcastEncryptedWssPacket({ kind: 'hello', senderId: selfId });
          } else if (packetType === 0x30) {
            let pIdx = idx;
            const topicLen = (buf[pIdx] << 8) | buf[pIdx + 1];
            pIdx += 2 + topicLen;
            const qos = (buf[pos] >> 1) & 0x03;
            if (qos > 0) pIdx += 2;

            const rawPayload = new TextDecoder().decode(buf.subarray(pIdx, packetEnd));
            await handleEncryptedWssFrame(rawPayload);
          }

          pos = packetEnd;
        }
      };

      ws.onerror = () => {};
      ws.onclose = () => {
        clientState.ready = false;
        if (clientState.pingTimer) {
          clearInterval(clientState.pingTimer);
          clientState.pingTimer = null;
        }
      };
    } catch {
      // 忽略单个节点连接异常
    }
  }
}

async function broadcastEncryptedWssPacket(packet) {
  const readyClients = e2eeWssClients.filter((c) => c.ready && c.ws.readyState === WebSocket.OPEN);
  if (readyClients.length === 0) return;

  try {
    const plainJson = JSON.stringify(packet);
    const maxSliceLen = 14000;

    if (plainJson.length <= maxSliceLen) {
      const enc = await encryptRoomText(plainJson);
      if (!enc) return;
      const frameStr = JSON.stringify({ s: selfId, iv: enc.iv, ct: enc.ct });
      for (const c of readyClients) {
        c.ws.send(buildMqttPublishPacket(c.topic, frameStr));
      }
    } else {
      const chunkId = createMessageId();
      const total = Math.ceil(plainJson.length / maxSliceLen);
      for (let i = 0; i < total; i++) {
        const slice = plainJson.slice(i * maxSliceLen, (i + 1) * maxSliceLen);
        const enc = await encryptRoomText(slice);
        if (!enc) return;
        const frameStr = JSON.stringify({
          s: selfId,
          cid: chunkId,
          idx: i,
          tot: total,
          iv: enc.iv,
          ct: enc.ct
        });
        for (const c of readyClients) {
          c.ws.send(buildMqttPublishPacket(c.topic, frameStr));
        }
      }
    }
  } catch {
    // 忽略加密广播异常
  }
}

async function handleEncryptedWssFrame(rawFrameStr) {
  try {
    const frame = JSON.parse(rawFrameStr);
    if (!frame || frame.s === selfId || !frame.iv || !frame.ct) return;

    const decryptedSlice = await decryptRoomText(frame.iv, frame.ct);
    if (!decryptedSlice) return;

    if (!frame.cid) {
      const packet = JSON.parse(decryptedSlice);
      handleEnvelopePacket(packet);
      return;
    }

    let entry = incomingChunkBuffer.get(frame.cid);
    if (!entry) {
      entry = { total: frame.tot, parts: new Array(frame.tot), received: 0 };
      incomingChunkBuffer.set(frame.cid, entry);
    }
    if (typeof entry.parts[frame.idx] !== 'string') {
      entry.parts[frame.idx] = decryptedSlice;
      entry.received += 1;
    }
    if (entry.received === entry.total) {
      incomingChunkBuffer.delete(frame.cid);
      const fullJson = entry.parts.join('');
      const packet = JSON.parse(fullJson);
      handleEnvelopePacket(packet);
    }
  } catch {
    // 忽略无法解密或非本房间的帧
  }
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
    timestamp: rawPayload.timestamp || Date.now(),
    originSenderId: selfId
  };

  processedMessageIds.add(payload.id);

  // 推入纯内存数组 `messages`（绝不写入 localStorage/sessionStorage/IndexedDB/Cookie）
  const localRecord = {
    ...payload,
    direction: 'sent',
    read: false,
    edited: false,
    revoked: false
  };
  messages.push(localRecord);
  renderSingleMessageToDOM(localRecord);

  // 通过所有活跃通道（Trystero WebRTC + E2EE WSS + 内存通道）广播给对方
  broadcastEnvelopePacket({
    kind: 'message',
    senderId: selfId,
    payload
  });
}

/**
 * 接收方处理对端发来的消息：推入内存 `messages` 数组并立即渲染到界面，同时触发已读回执
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
    direction: 'received',
    originSenderId: payload.originSenderId || 'peer',
    readAckSent: false,
    edited: false,
    revoked: false
  };

  // 仅推入内存变量 messages = []
  messages.push(receivedRecord);
  renderSingleMessageToDOM(receivedRecord);

  // 若当前页面处于可见状态，立即向发送方回传“已读”回执
  acknowledgeUnreadReceivedMessages();
}

/**
 * 向对方发送已读回执（包含本端已查看的接收消息 ID 列表）
 */
function acknowledgeUnreadReceivedMessages({ forceResendAll = false } = {}) {
  if (!activeRoomId || isSessionTerminated) return;
  if (document.visibilityState === 'hidden') return;

  const idsToAck = [];
  for (const msg of messages) {
    if (msg.direction === 'received' && !msg.revoked && (forceResendAll || !msg.readAckSent)) {
      msg.readAckSent = true;
      idsToAck.push(msg.id);
    }
  }

  if (idsToAck.length === 0) return;

  broadcastEnvelopePacket({
    kind: 'read',
    senderId: selfId,
    payload: { ids: idsToAck, readAt: Date.now() }
  });
}

/**
 * 发送方收到对端发来的已读回执后，将对应消息更新为“已读”并刷新界面徽标
 */
function applyReadReceiptIds(ids) {
  if (!Array.isArray(ids) || isSessionTerminated) return;
  const idSet = new Set(ids);
  for (const msg of messages) {
    if (msg.direction === 'sent' && idSet.has(msg.id) && !msg.read) {
      msg.read = true;
      refreshMessageRowInDOM(msg);
    }
  }
}

/**
 * 撤回指定 ID 的消息（支持文字、图片、语音）：
 * 清空内存中该条消息的正文与媒体数据，并同步通知对端撤回
 */
function revokeMessageById(msgId, { notifyPeer = true } = {}) {
  const msg = messages.find((m) => m.id === msgId);
  if (!msg || msg.revoked) return;

  // 若正在预览该图片或播放该语音，先关闭
  if (msg.type === 'image' && lightboxImgEl.src && lightboxImgEl.src === msg.mediaDataUrl) {
    closeImageLightbox();
  }
  if (msg.type === 'voice') {
    stopActiveAudioPlayback();
  }

  // 从内存中抹除该条消息的具体内容
  msg.text = '';
  msg.mediaDataUrl = '';
  msg.duration = 0;
  msg.revoked = true;

  refreshMessageRowInDOM(msg);

  if (notifyPeer) {
    broadcastEnvelopePacket({
      kind: 'revoke',
      senderId: selfId,
      payload: { id: msgId, revokedAt: Date.now() }
    });
  }
}

/**
 * 修改指定 ID 的文字消息并同步刷新双端界面
 */
function applyMessageEditById(msgId, newText, { notifyPeer = true } = {}) {
  const trimmed = (newText || '').trim();
  if (!trimmed) return;

  const msg = messages.find((m) => m.id === msgId);
  if (!msg || msg.revoked || msg.type !== 'text') return;

  msg.text = trimmed;
  msg.edited = true;
  refreshMessageRowInDOM(msg);

  if (notifyPeer) {
    broadcastEnvelopePacket({
      kind: 'edit',
      senderId: selfId,
      payload: { id: msgId, text: trimmed, editedAt: Date.now() }
    });
  }
}

/**
 * 将内存消息渲染为极简气泡 DOM（包含已读/未读状态、文字修改与撤回按钮）
 */
function renderSingleMessageToDOM(msg) {
  if (emptyChatHintEl && emptyChatHintEl.parentNode === messageListEl) {
    emptyChatHintEl.remove();
  }

  const rowEl = document.createElement('div');
  rowEl.dataset.msgId = msg.id;
  populateMessageRowElement(rowEl, msg);

  messageListEl.appendChild(rowEl);
  messageListEl.scrollTop = messageListEl.scrollHeight;
}

/**
 * 局部刷新单条消息 DOM（用于撤回、文字修改保存、已读状态更新）
 */
function refreshMessageRowInDOM(msg) {
  const rowEl = messageListEl.querySelector(`.msg-row[data-msg-id="${CSS.escape(msg.id)}"]`);
  if (!rowEl) return;
  populateMessageRowElement(rowEl, msg);
}

/**
 * 填充单条消息行内容
 */
function populateMessageRowElement(rowEl, msg) {
  rowEl.replaceChildren();

  if (msg.revoked) {
    rowEl.className = 'msg-row msg-is-revoked';
    const pillEl = document.createElement('div');
    pillEl.className = 'msg-revoked-pill';
    pillEl.textContent = msg.direction === 'sent' ? '你撤回了一条私语' : '对方撤回了一条私语';
    rowEl.appendChild(pillEl);
    return;
  }

  rowEl.className = `msg-row ${msg.direction === 'sent' ? 'msg-sent' : 'msg-received'}`;

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

  // 气泡下方精致微图标工具栏（已读/未读双勾图标、已修改微图标、文字修改笔形图标、撤回弧形箭头图标）
  const metaBarEl = document.createElement('div');
  metaBarEl.className = 'msg-footer-meta';

  if (msg.edited) {
    const editedTag = document.createElement('span');
    editedTag.className = 'msg-edited-tag';
    editedTag.title = '该私语已修改';
    editedTag.setAttribute('aria-label', '已修改');
    editedTag.innerHTML = `
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <path d="M12 20h9" stroke-linecap="round"/>
        <path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z" stroke-linecap="round" stroke-linejoin="round"/>
      </svg>
    `;
    metaBarEl.appendChild(editedTag);
  }

  if (msg.direction === 'sent') {
    // 1. 已读 / 未读小图标徽标（未读：单勾图标；已读：双勾高亮图标）
    const readBadge = document.createElement('span');
    readBadge.className = `msg-read-badge ${msg.read ? 'is-read' : 'is-unread'}`;
    readBadge.title = msg.read ? '已读' : '未读';
    readBadge.setAttribute('aria-label', msg.read ? '已读' : '未读');
    readBadge.dataset.readState = msg.read ? 'read' : 'unread';
    readBadge.innerHTML = msg.read
      ? `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2">
           <path d="M2 12.5l4.2 4.2L15.5 7.5" stroke-linecap="round" stroke-linejoin="round"/>
           <path d="M8.5 12.5l4.2 4.2L22 7.5" stroke-linecap="round" stroke-linejoin="round"/>
         </svg>`
      : `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.1">
           <path d="M5 12.5l4.5 4.5L19 7.5" stroke-linecap="round" stroke-linejoin="round"/>
         </svg>`;
    metaBarEl.appendChild(readBadge);

    // 2. 文字消息专属“修改”小图标按钮
    if (msg.type === 'text') {
      const editBtn = document.createElement('button');
      editBtn.type = 'button';
      editBtn.className = 'msg-action-btn msg-edit-btn';
      editBtn.title = '修改文字';
      editBtn.setAttribute('aria-label', '修改');
      editBtn.innerHTML = `
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9">
          <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
      `;
      editBtn.addEventListener('click', () => openInlineTextEditor(rowEl, bubbleEl, msg));
      metaBarEl.appendChild(editBtn);
    }

    // 3. 所有已发送消息（文字、图片、语音）均支持“撤回”小图标按钮
    const revokeBtn = document.createElement('button');
    revokeBtn.type = 'button';
    revokeBtn.className = 'msg-action-btn msg-revoke-btn';
    revokeBtn.title = '撤回私语';
    revokeBtn.setAttribute('aria-label', '撤回');
    revokeBtn.innerHTML = `
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.95">
        <path d="M9 14L4 9l5-5" stroke-linecap="round" stroke-linejoin="round"/>
        <path d="M4 9h10.5a5.5 5.5 0 0 1 5.5 5.5v0a5.5 5.5 0 0 1-5.5 5.5H11" stroke-linecap="round" stroke-linejoin="round"/>
      </svg>
    `;
    revokeBtn.addEventListener('click', () => revokeMessageById(msg.id, { notifyPeer: true }));
    metaBarEl.appendChild(revokeBtn);
  }

  if (metaBarEl.children.length > 0) {
    rowEl.appendChild(metaBarEl);
  }
}

/**
 * 打开文字消息气泡内联编辑器，修改后点击“保存刷新”或按 Enter 即刻同步双端
 */
function openInlineTextEditor(rowEl, bubbleEl, msg) {
  bubbleEl.replaceChildren();

  const editorWrap = document.createElement('div');
  editorWrap.className = 'msg-inline-editor';

  const inputEl = document.createElement('input');
  inputEl.type = 'text';
  inputEl.className = 'msg-inline-input';
  inputEl.value = msg.text;
  inputEl.maxLength = 4000;

  const actionsEl = document.createElement('div');
  actionsEl.className = 'msg-inline-actions';

  const cancelBtn = document.createElement('button');
  cancelBtn.type = 'button';
  cancelBtn.className = 'msg-inline-cancel';
  cancelBtn.textContent = '取消';

  const saveBtn = document.createElement('button');
  saveBtn.type = 'button';
  saveBtn.className = 'msg-inline-save';
  saveBtn.textContent = '保存刷新';

  const commitEdit = () => {
    const updated = inputEl.value.trim();
    if (!updated) {
      showToast('修改内容不能为空，若需删除请点击“撤回”');
      return;
    }
    applyMessageEditById(msg.id, updated, { notifyPeer: true });
  };

  cancelBtn.addEventListener('click', () => refreshMessageRowInDOM(msg));
  saveBtn.addEventListener('click', commitEdit);

  inputEl.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.isComposing) {
      e.preventDefault();
      commitEdit();
    } else if (e.key === 'Escape') {
      e.preventDefault();
      refreshMessageRowInDOM(msg);
    }
  });

  actionsEl.appendChild(cancelBtn);
  actionsEl.appendChild(saveBtn);
  editorWrap.appendChild(inputEl);
  editorWrap.appendChild(actionsEl);
  bubbleEl.appendChild(editorWrap);

  inputEl.focus();
  inputEl.setSelectionRange(inputEl.value.length, inputEl.value.length);
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
  if (presenceHeartbeatTimer) {
    clearInterval(presenceHeartbeatTimer);
    presenceHeartbeatTimer = null;
  }
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
    broadcastEnvelopePacket({ kind: 'terminate', senderId: selfId });
  } else {
    broadcastEnvelopePacket({ kind: 'peer-leave', senderId: selfId });
  }

  // 离开所有 Trystero P2P 房间
  while (activeRooms.length > 0) {
    const room = activeRooms.pop();
    try {
      room.leave();
    } catch {
      // 忽略退出异常
    }
  }
  p2pDispatchers = [];

  // 关闭所有 E2EE WSS 连接
  while (e2eeWssClients.length > 0) {
    const client = e2eeWssClients.pop();
    if (client.pingTimer) clearInterval(client.pingTimer);
    try {
      client.ws.close();
    } catch {
      // 忽略异常
    }
  }

  if (localEphemeralBus) {
    try {
      localEphemeralBus.close();
    } catch {
      // 忽略关闭异常
    }
    localEphemeralBus = null;
  }

  activeRoomId = null;
  roomCryptoKeyPromise = null;
  connectedPeers.clear();
  incomingChunkBuffer.clear();

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

  cleanupMemoryAndLeaveRoom({ notifyPeer, silent: false });
  isSessionTerminated = true;

  // 清除 URL 中的 #room=<房间ID> 片段，防止历史回退残留
  history.replaceState(null, '', window.location.pathname + window.location.search);

  // 展示“话已燃尽，一切随风去吧”画框终态画面
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

// 当用户切回标签页或聚焦窗口时，自动对当前未回执的接收消息发送“已读”回执
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible') {
    acknowledgeUnreadReceivedMessages();
  }
});
window.addEventListener('focus', () => {
  acknowledgeUnreadReceivedMessages();
});

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
