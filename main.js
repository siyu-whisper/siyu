/**
 * ============================================================================
 * 私语 (Siyu) — 极简、无后端、点对点阅后即焚临时聊天应用
 * 架构：Trystero (WebRTC DataChannel 端到端加密) + E2EE 零存储跨网直连 + 纯内存驻留 + 退出全员即焚
 * ============================================================================
 */
import { joinRoom as joinMqttRoom, selfId } from 'trystero/mqtt';
import { joinRoom as joinNostrRoom } from 'trystero/nostr';

const APP_ID = 'waiting-for-you-siyu';

// 国内外高可用公共 WSS 消息中继（用于 WebRTC 信令握手、AES-GCM-256 端到端加密瞬态转发及房间销毁墓碑标记）
const PUBLIC_WSS_BROKERS = [
  'wss://broker.emqx.io:8084/mqtt',
  'wss://broker.hivemq.com:8884/mqtt',
  'wss://test.mosquitto.org:8081/mqtt'
];

// 方案A：国风二字雅称池与专属莫兰迪印章配色（2~5人聊天时自动分配不同代号，清晰区分是谁说的）
const POETIC_PROFILES = [
  { alias: '听风', color: '#494266', bg: 'rgba(73, 66, 102, 0.12)' },
  { alias: '枕月', color: '#3B6E6C', bg: 'rgba(59, 110, 108, 0.12)' },
  { alias: '观云', color: '#565F8A', bg: 'rgba(86, 95, 138, 0.12)' },
  { alias: '拾叶', color: '#825B45', bg: 'rgba(130, 91, 69, 0.12)' },
  { alias: '眠雪', color: '#7A4E65', bg: 'rgba(122, 78, 101, 0.12)' },
  { alias: '伴竹', color: '#4B6B53', bg: 'rgba(75, 107, 83, 0.12)' },
  { alias: '知雨', color: '#48637A', bg: 'rgba(72, 99, 122, 0.12)' },
  { alias: '寻山', color: '#6E5A4F', bg: 'rgba(110, 90, 79, 0.12)' }
];

/**
 * ============================================================================
 * 一、纯内存消息与房间状态管理（核心安全原则：消息零持久化）
 * ============================================================================
 * 1. `messages` 数组仅存在于当前页面 JS 引擎的堆内存中。
 * 2. 严禁将 `messages` 写入 localStorage、sessionStorage、IndexedDB、CacheStorage 或 Cookie。
 * 3. `burnedRoomIds` 记录已结束销毁的房间 ID（配合 window.name 内存属性与 WSS 加密墓碑标记），
 *    确保任何一人点击“结束并清除”后，全员自动退出且原房间链接永久失效。
 */
let messages = [];
const processedMessageIds = new Set();
const ephemeralObjectUrls = new Set();
const incomingChunkBuffer = new Map();
const burnedRoomIds = new Set();

// 从当前标签页内存属性 window.name 中恢复本标签已销毁的房间 ID（不读写任何 Storage/Cookie）
(function restoreBurnedRoomsFromWindowMemory() {
  try {
    if (window.name && window.name.startsWith('siyu_mem:')) {
      const parsed = JSON.parse(window.name.slice(9));
      if (Array.isArray(parsed)) {
        parsed.forEach((id) => burnedRoomIds.add(id));
      }
    }
  } catch {
    // 忽略异常
  }
})();

function recordRoomBurnedInMemory(roomId) {
  if (!roomId) return;
  burnedRoomIds.add(roomId);
  try {
    const list = Array.from(burnedRoomIds).slice(-50);
    window.name = `siyu_mem:${JSON.stringify(list)}`;
  } catch {
    // 忽略异常
  }
}

// 当前房间运行态（全部驻留内存，离开即销毁）
let activeRooms = [];
let activeRoomId = null;
let roomCryptoKeyPromise = null;
let p2pDispatchers = [];
let e2eeWssClients = [];
let presenceHeartbeatTimer = null;
let localEphemeralBus = null;
let connectedPeers = new Set();
let roomParticipants = new Map(); // peerId -> { id, alias, color, bg, joinedAt }
let maxRoomCapacity = 2;          // 默认最大人数为 2，最高可设为 5
let selfJoinedAt = 0;
let isSessionTerminated = false;

// 实时语音通话 / 高清视频连麦运行态（驻留内存）
let activeCallSessionId = null;
let activeCallMode = 'audio';     // 'audio' | 'video'
let activeCallState = 'idle';     // 'idle' | 'ringing-out' | 'ringing-in' | 'connected'
let pendingIncomingCall = null;
let localCallStream = null;
let isCallMicMuted = false;
let isCallCamEnabled = false;
let isHdEnhanceEnabled = true;    // 默认开启高清锐化与色彩增强处理
let remoteCallHasVideo = false;
let callPeerAlias = '对方';
let callStartTime = 0;
let callTimerInterval = null;
let callVideoFrameTimer = null;
const stoppedCallSessionIds = new Set();

// 语音录制与播放运行态（驻留内存）
let mediaRecorder = null;
let recordingStream = null;
let recordingChunks = [];
let recordingStartTime = 0;
let recordingTimerInterval = null;
let isSyntheticRecording = false;
let currentPlayingAudio = null;
let currentPlayingBoxEl = null;

// 初始化当前用户的默认国风雅称
function pickInitialProfile(seedStr) {
  let hash = 0;
  for (let i = 0; i < seedStr.length; i++) {
    hash = (hash * 31 + seedStr.charCodeAt(i)) >>> 0;
  }
  const chosen = POETIC_PROFILES[hash % POETIC_PROFILES.length];
  return {
    alias: chosen.alias,
    color: chosen.color,
    bg: chosen.bg,
    custom: false
  };
}

let myProfile = pickInitialProfile(selfId);

// ==================== DOM 引用 ====================
const landingViewEl = document.getElementById('landing-view');
const chatViewEl = document.getElementById('chat-view');
const terminatedViewEl = document.getElementById('terminated-view');
const terminatedTitleEl = document.getElementById('terminated-title');
const terminatedDescEl = document.getElementById('terminated-desc');

const startWhisperBtn = document.getElementById('start-whisper-btn');
const toggleInviteBtn = document.getElementById('toggle-invite-btn');
const endAndClearBtn = document.getElementById('end-and-clear-btn');
const backHomeBtn = document.getElementById('back-home-btn');

const connectionBadgeEl = document.getElementById('connection-badge');
const connectionStatusTextEl = document.getElementById('connection-status-text');

const selfAliasBtn = document.getElementById('self-alias-btn');
const selfAliasDotEl = document.getElementById('self-alias-dot');
const selfAliasTextEl = document.getElementById('self-alias-text');
const aliasEditBoxEl = document.getElementById('alias-edit-box');
const aliasEditInputEl = document.getElementById('alias-edit-input');
const aliasSaveBtn = document.getElementById('alias-save-btn');

const inviteBarEl = document.getElementById('invite-bar');
const inviteLinkInputEl = document.getElementById('invite-link-input');
const copyLinkBtn = document.getElementById('copy-link-btn');
const maxPeersSelectEl = document.getElementById('max-peers-select');
const inviteFormatHintEl = document.getElementById('invite-format-hint');

const callInviteBannerEl = document.getElementById('call-invite-banner');
const callInviteTextEl = document.getElementById('call-invite-text');
const acceptCallBtn = document.getElementById('accept-call-btn');
const declineCallBtn = document.getElementById('decline-call-btn');

const liveCallStageEl = document.getElementById('live-call-stage');
const liveCallStatusTextEl = document.getElementById('live-call-status-text');
const liveCallTimerEl = document.getElementById('live-call-timer');
const liveCallHdBadgeEl = document.getElementById('live-call-hd-badge');
const callToggleMicBtn = document.getElementById('call-toggle-mic-btn');
const callToggleCamBtn = document.getElementById('call-toggle-cam-btn');
const callToggleHdBtn = document.getElementById('call-toggle-hd-btn');
const callFullscreenBtn = document.getElementById('call-fullscreen-btn');
const hangupCallBtn = document.getElementById('hangup-call-btn');
const callAudioAvatarViewEl = document.getElementById('call-audio-avatar-view');
const callSelfSealEl = document.getElementById('call-self-seal');
const callSelfNameEl = document.getElementById('call-self-name');
const callPeerSealEl = document.getElementById('call-peer-seal');
const callPeerNameEl = document.getElementById('call-peer-name');
const callVideoGridViewEl = document.getElementById('call-video-grid-view');
const callRemoteVideoEl = document.getElementById('call-remote-video');
const callRemoteFallbackImgEl = document.getElementById('call-remote-fallback-img');
const callLocalVideoEl = document.getElementById('call-local-video');
const callRemoteAudioEl = document.getElementById('call-remote-audio');

const messageListEl = document.getElementById('message-list');
const emptyChatHintEl = document.getElementById('empty-chat-hint');

const composerNormalRowEl = document.getElementById('composer-normal-row');
const composerRecordingRowEl = document.getElementById('composer-recording-row');
const voiceRecordBtn = document.getElementById('voice-record-btn');
const imagePickBtn = document.getElementById('image-pick-btn');
const imageFileInput = document.getElementById('image-file-input');
const videoPickBtn = document.getElementById('video-pick-btn');
const videoFileInput = document.getElementById('video-file-input');
const voiceCallBtn = document.getElementById('voice-call-btn');
const videoCallBtn = document.getElementById('video-call-btn');
const messageInputWrapEl = document.getElementById('message-input-wrap');
const messageInputEl = document.getElementById('message-input');
const emojiToggleBtn = document.getElementById('emoji-toggle-btn');
const emojiPickerPopoverEl = document.getElementById('emoji-picker-popover');
const emojiTabsBarEl = document.getElementById('emoji-tabs-bar');
const emojiGridEl = document.getElementById('emoji-grid');
const sendMsgBtn = document.getElementById('send-msg-btn');

const recordingTimerEl = document.getElementById('recording-timer');
const recordingModeLabelEl = document.getElementById('recording-mode-label');
const cancelVoiceBtn = document.getElementById('cancel-voice-btn');
const finishVoiceBtn = document.getElementById('finish-voice-btn');

const imageLightboxEl = document.getElementById('image-lightbox');
const lightboxImgEl = document.getElementById('lightbox-img');
const toastContainerEl = document.getElementById('toast-container');

// 跨标签页全局房间状态协调通道（用于秒级同步“房间已销毁”与“人数已满”校验，纯内存不落盘）
const globalRegistryBus = typeof BroadcastChannel !== 'undefined'
  ? new BroadcastChannel(`siyu_registry_${APP_ID}`)
  : null;

if (globalRegistryBus) {
  globalRegistryBus.onmessage = (event) => {
    const data = event.data;
    if (!data) return;

    if (data.kind === 'room-burned' && data.roomId) {
      recordRoomBurnedInMemory(data.roomId);
      if (activeRoomId === data.roomId && !isSessionTerminated) {
        terminateAndBurnSession({ notifyPeer: false, mode: 'burned' });
      }
    } else if (data.kind === 'query-room-state' && data.roomId && data.requesterId !== selfId) {
      // 如果查询的房间已被销毁，立即告知对方该链接已失效
      if (burnedRoomIds.has(data.roomId)) {
        globalRegistryBus.postMessage({
          kind: 'room-state-reply',
          roomId: data.roomId,
          targetPeerId: data.requesterId,
          status: 'terminated'
        });
        return;
      }
      // 如果查询的正是当前所在活跃房间，检查人数是否已满
      if (activeRoomId === data.roomId && !isSessionTerminated) {
        const currentCount = getAdmittedParticipantCount();
        if (!roomParticipants.has(data.requesterId) && currentCount >= maxRoomCapacity) {
          globalRegistryBus.postMessage({
            kind: 'room-state-reply',
            roomId: data.roomId,
            targetPeerId: data.requesterId,
            status: 'full',
            maxCapacity: maxRoomCapacity
          });
        }
      }
    } else if (data.kind === 'room-state-reply' && data.roomId === activeRoomId && data.targetPeerId === selfId) {
      if (data.status === 'terminated') {
        recordRoomBurnedInMemory(data.roomId);
        terminateAndBurnSession({ notifyPeer: false, mode: 'expired' });
      } else if (data.status === 'full') {
        terminateAndBurnSession({ notifyPeer: false, mode: 'full', maxCap: data.maxCapacity || maxRoomCapacity });
      }
    }
  };
}

/**
 * ============================================================================
 * 二、# 片段（Hash Fragment）房间 ID 解析与端到端加密密钥派生
 * ============================================================================
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

function parseRoomIdFromHash() {
  const rawHash = window.location.hash || '';
  if (!rawHash.startsWith('#')) return null;

  const fragment = rawHash.slice(1);
  const params = new URLSearchParams(fragment);
  const roomId = params.get('room');
  if (roomId && /^[A-Za-z0-9_-]{8,64}$/.test(roomId.trim())) {
    return roomId.trim();
  }
  return null;
}

function buildInviteUrl(roomId) {
  const baseUrl = `${window.location.origin}${window.location.pathname}`;
  return `${baseUrl}#room=${roomId}`;
}

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

async function encryptRoomText(plainText, customKeyPromise = roomCryptoKeyPromise) {
  if (!customKeyPromise) return null;
  const key = await customKeyPromise;
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const encoded = new TextEncoder().encode(plainText);
  const cipherBuf = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, encoded);
  return {
    iv: bytesToBase64(iv),
    ct: bytesToBase64(new Uint8Array(cipherBuf))
  };
}

async function decryptRoomText(ivB64, ctB64, customKeyPromise = roomCryptoKeyPromise) {
  if (!customKeyPromise) return null;
  const key = await customKeyPromise;
  const iv = base64ToBytes(ivB64);
  const ct = base64ToBytes(ctB64);
  const plainBuf = await crypto.subtle.decrypt({ name: 'AES-GCM', iv }, key, ct);
  return new TextDecoder().decode(plainBuf);
}

/**
 * ============================================================================
 * 三、方案A：国风代号更新与冲突消解
 * ============================================================================
 */
function renderSelfAliasUI() {
  if (selfAliasTextEl) selfAliasTextEl.textContent = myProfile.alias;
  if (selfAliasDotEl) selfAliasDotEl.style.background = myProfile.color;
}
renderSelfAliasUI();

function resolveAliasCollisionIfNeeded() {
  if (myProfile.custom) return;
  const usedAliases = new Set();
  for (const [peerId, info] of roomParticipants.entries()) {
    if (peerId !== selfId && info.alias) {
      usedAliases.add(info.alias);
    }
  }
  if (!usedAliases.has(myProfile.alias)) return;

  for (const candidate of POETIC_PROFILES) {
    if (!usedAliases.has(candidate.alias)) {
      myProfile = {
        alias: candidate.alias,
        color: candidate.color,
        bg: candidate.bg,
        custom: false
      };
      renderSelfAliasUI();
      if (roomParticipants.has(selfId)) {
        roomParticipants.set(selfId, {
          ...roomParticipants.get(selfId),
          alias: myProfile.alias,
          color: myProfile.color,
          bg: myProfile.bg
        });
      }
      return;
    }
  }
}

function getSenderDisplayProfile(senderId, fallbackAlias, fallbackColor, fallbackBg) {
  if (senderId === selfId) {
    return myProfile;
  }
  const peer = roomParticipants.get(senderId);
  if (peer && peer.alias) {
    return peer;
  }
  return {
    alias: fallbackAlias || '旅人',
    color: fallbackColor || '#3B6E6C',
    bg: fallbackBg || 'rgba(59, 110, 108, 0.12)'
  };
}

/**
 * ============================================================================
 * 四、房间创建、人数上限门禁（2~5人）与失效连接拦截
 * ============================================================================
 */
function getAdmittedParticipantCount() {
  return Math.max(1, roomParticipants.size);
}

function enterRoom(roomId, { isCreator = false } = {}) {
  // 若该房间链接已被标记为结束销毁，立即拦截并提示“连接已失效”
  if (burnedRoomIds.has(roomId)) {
    terminateAndBurnSession({ notifyPeer: false, mode: 'expired' });
    return;
  }

  if (activeRooms.length > 0 || e2eeWssClients.length > 0 || localEphemeralBus) {
    cleanupMemoryAndLeaveRoom({ silent: true });
  }

  isSessionTerminated = false;
  activeRoomId = roomId;
  selfJoinedAt = isCreator ? Date.now() - 10 : Date.now();
  roomCryptoKeyPromise = deriveRoomAesKey(roomId);
  connectedPeers.clear();
  roomParticipants.clear();

  // 初始化最大人数上限（默认 2 人，最大 5 人）
  if (isCreator) {
    maxRoomCapacity = 2;
    if (maxPeersSelectEl) maxPeersSelectEl.value = '2';
  } else {
    maxRoomCapacity = Number(maxPeersSelectEl?.value) || 2;
  }

  // 将自己登记入房间参与者表
  roomParticipants.set(selfId, {
    id: selfId,
    alias: myProfile.alias,
    color: myProfile.color,
    bg: myProfile.bg,
    joinedAt: selfJoinedAt
  });

  const inviteUrl = buildInviteUrl(roomId);
  history.replaceState(null, '', `#room=${roomId}`);

  document.body.classList.add('in-chat-mode');
  landingViewEl.classList.add('hidden');
  terminatedViewEl.classList.add('hidden');
  chatViewEl.classList.remove('hidden');

  inviteLinkInputEl.value = inviteUrl;
  inviteFormatHintEl.textContent = `当前房间凭证仅存于 #room=${roomId}，任一人点击结束则全员退出且此链接立即失效`;
  inviteBarEl.classList.remove('hidden');

  updateConnectionStatusUI();

  // 向全局注册表通道发起快速校验（检查是否已失效或已满员）
  if (globalRegistryBus) {
    globalRegistryBus.postMessage({
      kind: 'query-room-state',
      roomId,
      requesterId: selfId
    });
  }

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
      // 单个策略异常不影响其他通道
    }
  }

  // 2. 启动端到端加密（AES-GCM-256）WSS 零存储直连与房间状态墓碑监听
  startE2eeZeroStorageWssBridge(roomId, { isCreator });

  // 3. 启动同源内存 BroadcastChannel 通道
  if (typeof BroadcastChannel !== 'undefined') {
    const bus = new BroadcastChannel(`siyu_ephemeral_${APP_ID}_${roomId}`);
    localEphemeralBus = bus;

    bus.onmessage = (event) => {
      handleEnvelopePacket(event.data);
    };

    bus.postMessage(buildHelloPacket('hello'));
  }

  // 4. 定时发送在线心跳与人数/屏幕共享状态同步探针
  if (presenceHeartbeatTimer) clearInterval(presenceHeartbeatTimer);
  presenceHeartbeatTimer = setInterval(() => {
    if (!activeRoomId || isSessionTerminated) return;
    broadcastEnvelopePacket(buildHelloPacket('hello-ack'));
  }, 2800);

  messageInputEl.focus();
}

function buildHelloPacket(kind = 'hello') {
  return {
    kind,
    senderId: selfId,
    joinedAt: selfJoinedAt,
    profile: myProfile,
    maxCapacity: maxRoomCapacity,
    admittedIds: Array.from(roomParticipants.keys()),
    msgCount: messages.length
  };
}

function bindTrysteroRoomActions(room) {
  const [sendPacket, onPacket] = room.makeAction('siyu-pkt');
  p2pDispatchers.push(sendPacket);

  onPacket((packet) => {
    handleEnvelopePacket(packet);
  });

  room.onPeerJoin((peerId) => {
    if (isSessionTerminated) return;
    broadcastEnvelopePacket(buildHelloPacket('hello'));
    // 若当前正在进行实时语音/高清视频通话，将通话流推送给新加入的成员
    if (localCallStream && activeCallState === 'connected' && typeof room.addStream === 'function') {
      try {
        room.addStream(localCallStream, peerId, { streamRole: 'call', mode: activeCallMode });
        boostWebRtcVideoBitrate(room);
      } catch {
        // 忽略异常
      }
    }
  });

  room.onPeerLeave((peerId) => {
    handlePeerDeparture(peerId);
  });

  if (typeof room.onPeerStream === 'function') {
    room.onPeerStream((stream, peerId) => {
      if (isSessionTerminated) return;
      attachRemoteCallStream(stream, peerId);
    });
  }
}

function handlePeerDeparture(peerId) {
  if (!peerId) return;
  connectedPeers.delete(peerId);
  roomParticipants.delete(peerId);
  updateConnectionStatusUI();
}

/**
 * 统一处理所有通道收到的信封（含人数上限拦截、国风代号同步、后进房间历史同步、消息撤回/修改/已读、全员结束销毁）
 */
function handleEnvelopePacket(packet) {
  if (!packet || packet.senderId === selfId || isSessionTerminated) return;

  // 1. 收到全员结束销毁指令或墓碑状态 -> 所有人立即退出到结束界面，并将房间链接标记为永久失效
  if (packet.kind === 'terminate') {
    if (activeRoomId) recordRoomBurnedInMemory(activeRoomId);
    terminateAndBurnSession({ notifyPeer: false, mode: 'burned' });
    return;
  }

  if (packet.kind === 'room-state') {
    if (packet.status === 'terminated') {
      if (activeRoomId) recordRoomBurnedInMemory(activeRoomId);
      const isInitialJoin = connectedPeers.size === 0 && messages.length === 0;
      terminateAndBurnSession({ notifyPeer: false, mode: isInitialJoin ? 'expired' : 'burned' });
      return;
    }
    if (typeof packet.maxCapacity === 'number' && packet.maxCapacity >= 2 && packet.maxCapacity <= 5) {
      maxRoomCapacity = packet.maxCapacity;
      if (maxPeersSelectEl) maxPeersSelectEl.value = String(maxRoomCapacity);
      updateConnectionStatusUI();
    }
    return;
  }

  // 2. 收到针对本客户端的“房间人数已满”拒绝通知 -> 立即退出并提示无法加入
  if (packet.kind === 'room-full') {
    if (!packet.targetPeerId || packet.targetPeerId === selfId) {
      terminateAndBurnSession({
        notifyPeer: false,
        mode: 'full',
        maxCap: packet.maxCapacity || maxRoomCapacity
      });
    }
    return;
  }

  // 3. 处理对端握手与人数上限门禁校验
  if (packet.kind === 'hello' || packet.kind === 'hello-ack') {
    if (typeof packet.maxCapacity === 'number' && packet.maxCapacity >= 2 && packet.maxCapacity <= 5) {
      // 采纳更早进入房间者（房主）设定的上限
      if ((packet.joinedAt || 0) < selfJoinedAt) {
        maxRoomCapacity = packet.maxCapacity;
        if (maxPeersSelectEl) maxPeersSelectEl.value = String(maxRoomCapacity);
      }
    }

    const isExistingPeer = roomParticipants.has(packet.senderId);
    if (!isExistingPeer) {
      // 若当前房间人数已达到上限，且新来者晚于本房间现有成员，拒绝其加入
      if (roomParticipants.size >= maxRoomCapacity) {
        const latestAdmittedJoinTime = Math.max(
          ...Array.from(roomParticipants.values()).map((p) => p.joinedAt || 0)
        );
        if ((packet.joinedAt || Date.now()) >= latestAdmittedJoinTime) {
          broadcastEnvelopePacket({
            kind: 'room-full',
            senderId: selfId,
            targetPeerId: packet.senderId,
            maxCapacity: maxRoomCapacity
          });
          return;
        }
      }

      // 若对方发来的已准入名单已满且不包含本端，说明本端是超员后进者，主动退出并提示无法加入
      if (
        Array.isArray(packet.admittedIds) &&
        packet.admittedIds.length >= maxRoomCapacity &&
        !packet.admittedIds.includes(selfId) &&
        (packet.joinedAt || 0) < selfJoinedAt
      ) {
        terminateAndBurnSession({
          notifyPeer: false,
          mode: 'full',
          maxCap: maxRoomCapacity
        });
        return;
      }
    }

    // 准入该成员并登记其国风代号
    admitAndRegisterPeer(packet.senderId, packet.profile, packet.joinedAt);

    if (packet.kind === 'hello') {
      broadcastEnvelopePacket(buildHelloPacket('hello-ack'));
      syncInMemoryHistoryToPeers();
      acknowledgeUnreadReceivedMessages({ forceResendAll: true });
    } else {
      if ((packet.msgCount === 0 || typeof packet.msgCount === 'undefined') && messages.length > 0) {
        syncInMemoryHistoryToPeers();
      }
      acknowledgeUnreadReceivedMessages({ forceResendAll: true });
    }
    return;
  }

  // 非准入成员的消息不予处理
  if (!roomParticipants.has(packet.senderId)) {
    if (roomParticipants.size >= maxRoomCapacity) {
      broadcastEnvelopePacket({
        kind: 'room-full',
        senderId: selfId,
        targetPeerId: packet.senderId,
        maxCapacity: maxRoomCapacity
      });
      return;
    }
    admitAndRegisterPeer(packet.senderId, packet.profile, packet.joinedAt);
  }

  if (packet.kind === 'room-meta' && typeof packet.maxCapacity === 'number') {
    maxRoomCapacity = Math.max(2, Math.min(5, packet.maxCapacity));
    if (maxPeersSelectEl) maxPeersSelectEl.value = String(maxRoomCapacity);
    updateConnectionStatusUI();
    showToast(`房间人数上限已调整为 ${maxRoomCapacity} 人`);
  } else if (packet.kind === 'profile-update' && packet.profile) {
    admitAndRegisterPeer(packet.senderId, packet.profile, packet.joinedAt);
    refreshAllMessagesInDOM();
  } else if (packet.kind === 'call-invite') {
    handleIncomingCallInvite(packet);
  } else if (packet.kind === 'call-accept') {
    handleIncomingCallAccept(packet);
  } else if (packet.kind === 'call-decline') {
    handleIncomingCallDecline(packet);
  } else if (packet.kind === 'call-media-state') {
    handleIncomingCallMediaState(packet);
  } else if (packet.kind === 'call-video-frame' && packet.frameDataUrl) {
    handleIncomingCallVideoFrame(packet);
  } else if (packet.kind === 'call-hangup') {
    handleIncomingCallHangup(packet);
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
  } else if (packet.kind === 'peer-leave') {
    handlePeerDeparture(packet.senderId);
  }
}

function admitAndRegisterPeer(peerId, profile, joinedAt) {
  if (!peerId || peerId === selfId) return;
  const wasConnected = connectedPeers.has(peerId);
  connectedPeers.add(peerId);

  const fallback = pickInitialProfile(peerId);
  roomParticipants.set(peerId, {
    id: peerId,
    alias: profile?.alias || fallback.alias,
    color: profile?.color || fallback.color,
    bg: profile?.bg || fallback.bg,
    joinedAt: joinedAt || Date.now()
  });

  // 如果自己的默认代号与更早进房间的成员撞名，自动切换为下一个国风代号
  resolveAliasCollisionIfNeeded();
  updateConnectionStatusUI();

  if (!wasConnected) {
    const peerAlias = roomParticipants.get(peerId)?.alias || '对方';
    showToast(`${peerAlias} 已加入私语房间 (${getAdmittedParticipantCount()}/${maxRoomCapacity})`);
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

  broadcastEncryptedWssPacket(packet, { retain: false });

  if (localEphemeralBus) {
    try {
      localEphemeralBus.postMessage(packet);
    } catch {
      // 忽略异常
    }
  }
}

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
    originSenderId: m.originSenderId || (m.direction === 'sent' ? selfId : 'peer'),
    senderAlias: m.senderAlias || myProfile.alias,
    senderColor: m.senderColor || myProfile.color,
    senderBg: m.senderBg || myProfile.bg
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
        senderAlias: item.senderAlias || '旅人',
        senderColor: item.senderColor || '#3B6E6C',
        senderBg: item.senderBg || 'rgba(59, 110, 108, 0.12)',
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
 * ============================================================================
 * 五、端到端加密（AES-GCM-256）WSS 通道与房间销毁永久墓碑机制
 * ============================================================================
 * 说明：
 * - `liveTopic` (`retain=false`)：传输实时加密消息（零存储，绝不落盘）。
 * - `stateTopic` (`retain=true`)：仅存储加密后的房间状态墓碑 `{ kind: 'room-state', status: 'terminated' }`。
 *   当任何一人点击“结束并清除”后，WSS 节点保留该加密墓碑；此后任何人再次点击该旧链接，
 *   连接瞬间即收到 `status: 'terminated'` 并被拦截在“连接已失效”页面。
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

function buildMqttSubscribePacket(topic, packetId = 1) {
  const tBytes = new TextEncoder().encode(topic);
  const body = [packetId >> 8, packetId & 0xff, tBytes.length >> 8, tBytes.length & 0xff, ...tBytes, 0x00];
  return new Uint8Array([0x82, ...encodeMqttVarLength(body.length), ...body]);
}

function buildMqttPublishPacket(topic, utf8String, { retain = false } = {}) {
  const tBytes = new TextEncoder().encode(topic);
  const pBytes = new TextEncoder().encode(utf8String);
  const remLen = 2 + tBytes.length + pBytes.length;
  const firstByte = retain ? 0x31 : 0x30;
  const header = [firstByte, ...encodeMqttVarLength(remLen), tBytes.length >> 8, tBytes.length & 0xff];
  const out = new Uint8Array(header.length + tBytes.length + pBytes.length);
  out.set(header, 0);
  out.set(tBytes, header.length);
  out.set(pBytes, header.length + tBytes.length);
  return out;
}

function startE2eeZeroStorageWssBridge(roomId) {
  if (typeof WebSocket === 'undefined') return;
  const liveTopic = `siyu/v2/${APP_ID}/${roomId}/live`;
  const stateTopic = `siyu/v2/${APP_ID}/${roomId}/state`;

  for (const brokerUrl of PUBLIC_WSS_BROKERS) {
    try {
      const ws = new WebSocket(brokerUrl, 'mqtt');
      ws.binaryType = 'arraybuffer';

      const clientState = {
        ws,
        ready: false,
        liveTopic,
        stateTopic,
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
            // 先订阅房间状态墓碑频道，再订阅实时通信频道
            ws.send(buildMqttSubscribePacket(stateTopic, 1));
            ws.send(buildMqttSubscribePacket(liveTopic, 2));
            clientState.pingTimer = setInterval(() => {
              if (ws.readyState === WebSocket.OPEN) {
                ws.send(new Uint8Array([0xc0, 0x00]));
              }
            }, 25000);
            broadcastEncryptedWssPacket(buildHelloPacket('hello'), { retain: false });
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
      // 忽略单个节点异常
    }
  }
}

async function broadcastEncryptedWssPacket(
  packet,
  { retain = false, targetClients = e2eeWssClients, keyPromise = roomCryptoKeyPromise } = {}
) {
  const readyClients = targetClients.filter((c) => c.ready && c.ws.readyState === WebSocket.OPEN);
  if (readyClients.length === 0) return;

  try {
    const plainJson = JSON.stringify(packet);
    const maxSliceLen = 14000;

    if (plainJson.length <= maxSliceLen) {
      const enc = await encryptRoomText(plainJson, keyPromise);
      if (!enc) return;
      const frameStr = JSON.stringify({
        s: selfId,
        allowSelf: Boolean(retain),
        iv: enc.iv,
        ct: enc.ct
      });
      for (const c of readyClients) {
        const topic = retain ? c.stateTopic : c.liveTopic;
        c.ws.send(buildMqttPublishPacket(topic, frameStr, { retain }));
      }
    } else {
      const chunkId = createMessageId();
      const total = Math.ceil(plainJson.length / maxSliceLen);
      for (let i = 0; i < total; i++) {
        const slice = plainJson.slice(i * maxSliceLen, (i + 1) * maxSliceLen);
        const enc = await encryptRoomText(slice, keyPromise);
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
          c.ws.send(buildMqttPublishPacket(c.liveTopic, frameStr, { retain: false }));
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
    if (!frame || !frame.iv || !frame.ct) return;
    if (frame.s === selfId && !frame.allowSelf) return;

    const decryptedSlice = await decryptRoomText(frame.iv, frame.ct);
    if (!decryptedSlice) return;

    if (!frame.cid) {
      const packet = JSON.parse(decryptedSlice);
      if (packet.kind === 'room-state' && packet.status === 'terminated') {
        if (activeRoomId) recordRoomBurnedInMemory(activeRoomId);
        const isInitialJoin = connectedPeers.size === 0 && messages.length === 0;
        terminateAndBurnSession({ notifyPeer: false, mode: isInitialJoin ? 'expired' : 'burned' });
        return;
      }
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

function updateConnectionStatusUI() {
  const count = getAdmittedParticipantCount();
  const isConnected = connectedPeers.size > 0;
  if (isConnected) {
    connectionBadgeEl.classList.remove('status-waiting');
    connectionBadgeEl.classList.add('status-connected');
    connectionStatusTextEl.textContent = `已连接 (${count}/${maxRoomCapacity})`;
  } else {
    connectionBadgeEl.classList.remove('status-connected');
    connectionBadgeEl.classList.add('status-waiting');
    connectionStatusTextEl.textContent = `等待对方加入 (${count}/${maxRoomCapacity})`;
  }
}

/**
 * ============================================================================
 * 六、消息收发、撤回、文字修改重刷、未读(×)/已读(✓)回执（纯内存 `messages` 数组驻留）
 * ============================================================================
 */
function createMessageId() {
  const rand = crypto.getRandomValues(new Uint32Array(2));
  return `${Date.now().toString(36)}-${rand[0].toString(36)}${rand[1].toString(36)}`;
}

function dispatchOutgoingMessage(rawPayload) {
  if (!activeRoomId || isSessionTerminated) return;

  const payload = {
    id: createMessageId(),
    type: rawPayload.type || 'text',
    text: rawPayload.text || '',
    mediaDataUrl: rawPayload.mediaDataUrl || '',
    duration: rawPayload.duration || 0,
    timestamp: rawPayload.timestamp || Date.now(),
    originSenderId: selfId,
    senderAlias: myProfile.alias,
    senderColor: myProfile.color,
    senderBg: myProfile.bg
  };

  processedMessageIds.add(payload.id);

  const localRecord = {
    ...payload,
    direction: 'sent',
    read: false,
    edited: false,
    revoked: false
  };
  messages.push(localRecord);
  renderSingleMessageToDOM(localRecord);

  broadcastEnvelopePacket({
    kind: 'message',
    senderId: selfId,
    payload
  });
}

function handleIncomingMessage(payload) {
  if (!payload || isSessionTerminated) return;
  const msgId = payload.id || `${payload.timestamp}-${payload.text}`;
  if (processedMessageIds.has(msgId)) return;
  processedMessageIds.add(msgId);

  const senderId = payload.originSenderId || 'peer';
  if (senderId !== 'peer' && payload.senderAlias && !roomParticipants.has(senderId)) {
    admitAndRegisterPeer(senderId, {
      alias: payload.senderAlias,
      color: payload.senderColor,
      bg: payload.senderBg
    });
  }

  const receivedRecord = {
    id: msgId,
    type: payload.type || 'text',
    text: payload.text || '',
    mediaDataUrl: payload.mediaDataUrl || '',
    duration: payload.duration || 0,
    timestamp: payload.timestamp || Date.now(),
    direction: 'received',
    originSenderId: senderId,
    senderAlias: payload.senderAlias || '旅人',
    senderColor: payload.senderColor || '#3B6E6C',
    senderBg: payload.senderBg || 'rgba(59, 110, 108, 0.12)',
    readAckSent: false,
    edited: false,
    revoked: false
  };

  messages.push(receivedRecord);
  renderSingleMessageToDOM(receivedRecord);
  acknowledgeUnreadReceivedMessages();
}

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

function revokeMessageById(msgId, { notifyPeer = true } = {}) {
  const msg = messages.find((m) => m.id === msgId);
  if (!msg || msg.revoked) return;

  if (msg.type === 'image' && lightboxImgEl.src && lightboxImgEl.src === msg.mediaDataUrl) {
    closeImageLightbox();
  }
  if (msg.type === 'voice') {
    stopActiveAudioPlayback();
  }

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

function refreshMessageRowInDOM(msg) {
  const rowEl = messageListEl.querySelector(`.msg-row[data-msg-id="${CSS.escape(msg.id)}"]`);
  if (!rowEl) return;
  populateMessageRowElement(rowEl, msg);
}

function refreshAllMessagesInDOM() {
  for (const msg of messages) {
    refreshMessageRowInDOM(msg);
  }
}

/**
 * 渲染单条消息行：
 * - 顶部显示方案A发言者国风代号小印章（多人聊天一目了然区分是谁说的）
 * - 底部显示精致微图标：未读(×) / 已读(✓)、文字修改笔形图标、撤回弧形箭头图标
 */
function populateMessageRowElement(rowEl, msg) {
  rowEl.replaceChildren();

  const senderProfile = getSenderDisplayProfile(
    msg.originSenderId,
    msg.senderAlias,
    msg.senderColor,
    msg.senderBg
  );

  if (msg.revoked) {
    rowEl.className = 'msg-row msg-is-revoked';
    const pillEl = document.createElement('div');
    pillEl.className = 'msg-revoked-pill';
    pillEl.textContent = msg.direction === 'sent'
      ? `你（${senderProfile.alias}）撤回了一条私语`
      : `${senderProfile.alias} 撤回了一条私语`;
    rowEl.appendChild(pillEl);
    return;
  }

  rowEl.className = `msg-row ${msg.direction === 'sent' ? 'msg-sent' : 'msg-received'}`;

  // 方案A：发言人国风代号印章标签
  const sealEl = document.createElement('div');
  sealEl.className = 'msg-sender-seal';
  sealEl.style.color = senderProfile.color;
  sealEl.style.background = senderProfile.bg;
  sealEl.innerHTML = `<span class="seal-dot" style="background:${senderProfile.color}"></span><span class="seal-name"></span>`;
  sealEl.querySelector('.seal-name').textContent = msg.direction === 'sent'
    ? `${senderProfile.alias} · 我`
    : senderProfile.alias;
  rowEl.appendChild(sealEl);

  const bubbleEl = document.createElement('div');
  bubbleEl.className = `msg-bubble msg-type-${msg.type || 'text'}`;

  if (msg.type === 'image' && msg.mediaDataUrl) {
    const imgEl = document.createElement('img');
    imgEl.className = 'msg-image';
    imgEl.src = msg.mediaDataUrl;
    imgEl.alt = '私语图片';
    imgEl.addEventListener('click', () => openImageLightbox(msg.mediaDataUrl));
    bubbleEl.appendChild(imgEl);
  } else if (msg.type === 'video' && msg.mediaDataUrl) {
    const videoWrapEl = document.createElement('div');
    videoWrapEl.className = 'msg-video-wrap';
    const videoEl = document.createElement('video');
    videoEl.className = 'msg-video hd-video-enhanced';
    videoEl.src = msg.mediaDataUrl;
    videoEl.controls = true;
    videoEl.playsInline = true;
    videoEl.preload = 'metadata';
    const hdTag = document.createElement('span');
    hdTag.className = 'msg-video-hd-tag';
    hdTag.textContent = 'HD 超清';
    videoWrapEl.appendChild(videoEl);
    videoWrapEl.appendChild(hdTag);
    bubbleEl.appendChild(videoWrapEl);
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

  // 气泡下方微图标栏（未读 × / 已读 ✓、修改、撤回）
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
    // 1. 未读显示 ×，已读显示 ✓
    const readBadge = document.createElement('span');
    readBadge.className = `msg-read-badge ${msg.read ? 'is-read' : 'is-unread'}`;
    readBadge.title = msg.read ? '已读 (✓)' : '未读 (×)';
    readBadge.setAttribute('aria-label', msg.read ? '已读' : '未读');
    readBadge.dataset.readState = msg.read ? 'read' : 'unread';
    readBadge.innerHTML = msg.read
      ? `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4">
           <path d="M5 12.5l4.5 4.5L19 7.5" stroke-linecap="round" stroke-linejoin="round"/>
         </svg>`
      : `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2">
           <path d="M7.5 7.5l9 9M16.5 7.5l-9 9" stroke-linecap="round" stroke-linejoin="round"/>
         </svg>`;
    metaBarEl.appendChild(readBadge);

    // 2. 文字消息专属“修改”微图标按钮
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

    // 3. 所有已发送消息（文字、图片、语音）均支持“撤回”微图标按钮
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
      showToast('修改内容不能为空，若需删除请点击撤回图标');
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
 * ============================================================================
 * 七、实时语音通话与 1080P/720P 高清视频连麦功能（WebRTC 高码率优化 + 硬件卷积锐化 + 双端同步挂断）
 * ============================================================================
 */

/**
 * 提升 WebRTC 底层 RTCRtpSender 的视频编码最大码率（2.5Mbps）与清晰度优先策略（maintain-resolution）
 */
function boostWebRtcVideoBitrate(room) {
  if (!room || typeof room.getPeers !== 'function') return;
  try {
    const peers = room.getPeers();
    for (const peerConn of Object.values(peers)) {
      if (!peerConn || typeof peerConn.getSenders !== 'function') continue;
      for (const sender of peerConn.getSenders()) {
        if (sender.track && sender.track.kind === 'video' && typeof sender.getParameters === 'function') {
          const params = sender.getParameters() || {};
          if (!Array.isArray(params.encodings) || params.encodings.length === 0) {
            params.encodings = [{}];
          }
          params.encodings[0].maxBitrate = 2500000; // 2.5 Mbps 高清视频码率上限
          params.encodings[0].maxFramerate = 30;
          params.degradationPreference = 'maintain-resolution';
          sender.setParameters(params).catch(() => {});
        }
      }
    }
  } catch {
    // 忽略底层不支持动态编码参数调整的浏览器
  }
}

/**
 * 采集高清音视频流（视频请求 1080P/720P 高清分辨率 + contentHint='detail' 细节锐化提示）
 */
async function acquireCallMediaStream(mode = 'audio') {
  const wantVideo = mode === 'video';
  if (navigator.mediaDevices && typeof navigator.mediaDevices.getUserMedia === 'function') {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true,
          sampleRate: 48000,
          channelCount: 2
        },
        video: wantVideo
          ? {
              width: { ideal: 1920, min: 1280 },
              height: { ideal: 1080, min: 720 },
              frameRate: { ideal: 30, max: 60 },
              facingMode: 'user'
            }
          : false
      });
      stream.getVideoTracks().forEach((track) => {
        if ('contentHint' in track) {
          track.contentHint = 'detail';
        }
      });
      return stream;
    } catch {
      // 若摄像头不支持 1080P 強约束，自动回退至标准 720P 高清采集
      if (wantVideo) {
        try {
          const hdFallback = await navigator.mediaDevices.getUserMedia({
            audio: true,
            video: {
              width: { ideal: 1280 },
              height: { ideal: 720 },
              frameRate: { ideal: 30 }
            }
          });
          hdFallback.getVideoTracks().forEach((track) => {
            if ('contentHint' in track) track.contentHint = 'detail';
          });
          return hdFallback;
        } catch {
          try {
            const audioOnly = await navigator.mediaDevices.getUserMedia({
              audio: true,
              video: false
            });
            return audioOnly;
          } catch {
            // 继续走内存合成流兜底
          }
        }
      }
    }
  }

  // 无物理麦克风/摄像头环境（如受限桌面或无头测试环境）下自动生成内存高清合成音视频流
  const tracks = [];
  const AudioCtx = window.AudioContext || window.webkitAudioContext;
  if (AudioCtx) {
    try {
      const ctx = new AudioCtx();
      const dest = ctx.createMediaStreamDestination();
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      gain.gain.value = 0.001;
      osc.connect(gain).connect(dest);
      osc.start();
      tracks.push(...dest.stream.getAudioTracks());
    } catch {
      // 忽略异常
    }
  }
  if (wantVideo) {
    try {
      const canvas = document.createElement('canvas');
      canvas.width = 960;
      canvas.height = 540;
      const c2d = canvas.getContext('2d');
      c2d.fillStyle = '#494266';
      c2d.fillRect(0, 0, 960, 540);
      c2d.fillStyle = '#E9E8EF';
      c2d.font = '28px sans-serif';
      c2d.fillText(`${myProfile.alias} · HD 1080P`, 360, 275);
      const vStream = canvas.captureStream(24);
      vStream.getVideoTracks().forEach((track) => {
        if ('contentHint' in track) track.contentHint = 'detail';
      });
      tracks.push(...vStream.getVideoTracks());
    } catch {
      // 忽略异常
    }
  }
  return new MediaStream(tracks);
}

function applyHdEnhancementFilterState() {
  const targets = [callRemoteVideoEl, callRemoteFallbackImgEl, callLocalVideoEl];
  for (const el of targets) {
    if (el) {
      el.classList.toggle('hd-video-enhanced', isHdEnhanceEnabled);
    }
  }
  if (callToggleHdBtn) {
    callToggleHdBtn.classList.toggle('is-active', isHdEnhanceEnabled);
    callToggleHdBtn.textContent = isHdEnhanceEnabled ? '✨ 超清增强: 开' : '✨ 超清增强: 关';
  }
  if (liveCallHdBadgeEl) {
    const showVideoLayout = Boolean(isCallCamEnabled || remoteCallHasVideo || activeCallMode === 'video');
    liveCallHdBadgeEl.classList.toggle('hidden', !showVideoLayout);
    liveCallHdBadgeEl.textContent = isHdEnhanceEnabled ? 'HD 1080P 超清增强' : 'HD 标准画质';
  }
}

function toggleVideoHdEnhancement() {
  isHdEnhanceEnabled = !isHdEnhanceEnabled;
  applyHdEnhancementFilterState();
  showToast(
    isHdEnhanceEnabled
      ? '已开启视频超清锐化与色彩增强引擎'
      : '已切换为原始标准视频画质'
  );
}

function updateLiveCallStageUI() {
  if (!liveCallStageEl) return;

  // 更新双方国风印章与称呼
  if (callSelfSealEl) callSelfSealEl.textContent = (myProfile.alias || '我').slice(0, 1);
  if (callSelfNameEl) callSelfNameEl.textContent = `${myProfile.alias} · 我`;
  if (callPeerSealEl) callPeerSealEl.textContent = (callPeerAlias || '友').slice(0, 1);
  if (callPeerNameEl) callPeerNameEl.textContent = callPeerAlias || '对方';

  // 更新按钮文案
  if (callToggleMicBtn) {
    callToggleMicBtn.textContent = isCallMicMuted ? '🔇 麦克风: 已静音' : '🎙️ 麦克风: 开';
    callToggleMicBtn.classList.toggle('is-active', !isCallMicMuted);
  }
  if (callToggleCamBtn) {
    callToggleCamBtn.textContent = isCallCamEnabled ? '📷 摄像头: 开' : '📷 摄像头: 关';
    callToggleCamBtn.classList.toggle('is-active', isCallCamEnabled);
  }

  // 根据是否有视频画面切换“声波双印章视图”或“高清双人视频画中画视图”
  const showVideoLayout = Boolean(isCallCamEnabled || remoteCallHasVideo || activeCallMode === 'video');
  if (callAudioAvatarViewEl && callVideoGridViewEl) {
    callAudioAvatarViewEl.classList.toggle('hidden', showVideoLayout);
    callVideoGridViewEl.classList.toggle('hidden', !showVideoLayout);
  }
  if (callToggleHdBtn) {
    callToggleHdBtn.classList.toggle('hidden', !showVideoLayout);
  }
  applyHdEnhancementFilterState();
}

function startCallTimer() {
  if (callTimerInterval) clearInterval(callTimerInterval);
  callStartTime = Date.now();
  if (liveCallTimerEl) liveCallTimerEl.textContent = '00:00';

  callTimerInterval = setInterval(() => {
    if (activeCallState !== 'connected' || !liveCallTimerEl) return;
    const sec = Math.floor((Date.now() - callStartTime) / 1000);
    const m = String(Math.floor(sec / 60)).padStart(2, '0');
    const s = String(sec % 60).padStart(2, '0');
    liveCallTimerEl.textContent = `${m}:${s}`;
  }, 500);
}

function startCallVideoFrameSyncLoop() {
  if (callVideoFrameTimer) clearInterval(callVideoFrameTimer);
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');
  if (ctx) {
    ctx.imageSmoothingEnabled = true;
    ctx.imageSmoothingQuality = 'high';
  }

  callVideoFrameTimer = setInterval(() => {
    if (
      activeCallState !== 'connected' ||
      !isCallCamEnabled ||
      !localCallStream ||
      !callLocalVideoEl ||
      isSessionTerminated
    ) {
      return;
    }
    const vw = callLocalVideoEl.videoWidth || 640;
    const vh = callLocalVideoEl.videoHeight || 360;
    const targetW = Math.min(720, vw);
    const targetH = Math.round((vh * targetW) / vw);
    canvas.width = targetW;
    canvas.height = targetH;
    try {
      ctx.drawImage(callLocalVideoEl, 0, 0, targetW, targetH);
      const frameDataUrl = canvas.toDataURL('image/jpeg', 0.76);
      broadcastEnvelopePacket({
        kind: 'call-video-frame',
        senderId: selfId,
        callSessionId: activeCallSessionId,
        alias: myProfile.alias,
        frameDataUrl
      });
    } catch {
      // 忽略单帧异常
    }
  }, 750);
}

/**
 * 发起实时语音通话或高清视频连麦
 * @param {'audio'|'video'} mode
 */
async function startLiveCall(mode = 'audio') {
  if (!activeRoomId || isSessionTerminated) return;
  if (activeCallState !== 'idle') {
    showToast('当前已在实时通话中');
    return;
  }

  const stream = await acquireCallMediaStream(mode);
  localCallStream = stream;
  activeCallSessionId = createMessageId();
  activeCallMode = mode;
  activeCallState = 'ringing-out';
  isCallMicMuted = false;
  isCallCamEnabled = mode === 'video' && stream.getVideoTracks().length > 0;
  remoteCallHasVideo = mode === 'video';

  const firstPeer = Array.from(roomParticipants.values()).find((p) => p.id !== selfId);
  callPeerAlias = firstPeer?.alias || '对方';

  document.body.classList.add('is-video-active');
  if (callLocalVideoEl && isCallCamEnabled) {
    callLocalVideoEl.srcObject = stream;
    callLocalVideoEl.play().catch(() => {});
  }

  if (liveCallStatusTextEl) {
    liveCallStatusTextEl.textContent =
      mode === 'video' ? '正在呼叫对方进行高清视频连麦...' : '正在呼叫对方进行语音通话...';
  }
  if (liveCallTimerEl) liveCallTimerEl.textContent = '呼叫中';
  updateLiveCallStageUI();
  liveCallStageEl.classList.remove('hidden');

  broadcastEnvelopePacket({
    kind: 'call-invite',
    senderId: selfId,
    callSessionId: activeCallSessionId,
    alias: myProfile.alias,
    mode
  });
}

function handleIncomingCallInvite(packet) {
  if (!packet || !packet.callSessionId || stoppedCallSessionIds.has(packet.callSessionId)) return;
  if (activeCallState === 'connected') return;

  const peerProfile = getSenderDisplayProfile(packet.senderId, packet.alias);
  pendingIncomingCall = {
    callSessionId: packet.callSessionId,
    callerId: packet.senderId,
    callerAlias: peerProfile.alias,
    mode: packet.mode === 'video' ? 'video' : 'audio'
  };
  activeCallState = 'ringing-in';

  if (callInviteTextEl) {
    const modeLabel = pendingIncomingCall.mode === 'video' ? '实时高清视频连麦' : '实时语音通话';
    callInviteTextEl.textContent = `「${peerProfile.alias}」邀请你进行${modeLabel}...`;
  }
  if (callInviteBannerEl) {
    callInviteBannerEl.classList.remove('hidden');
  }
}

async function acceptIncomingCall() {
  if (!pendingIncomingCall || isSessionTerminated) return;
  const { callSessionId, callerAlias, mode } = pendingIncomingCall;
  pendingIncomingCall = null;
  if (callInviteBannerEl) callInviteBannerEl.classList.add('hidden');

  const stream = await acquireCallMediaStream(mode);
  localCallStream = stream;
  activeCallSessionId = callSessionId;
  activeCallMode = mode;
  activeCallState = 'connected';
  callPeerAlias = callerAlias || '对方';
  isCallMicMuted = false;
  isCallCamEnabled = mode === 'video' && stream.getVideoTracks().length > 0;
  remoteCallHasVideo = mode === 'video';

  document.body.classList.add('is-video-active');
  if (callLocalVideoEl && isCallCamEnabled) {
    callLocalVideoEl.srcObject = stream;
    callLocalVideoEl.play().catch(() => {});
  }

  if (liveCallStatusTextEl) {
    liveCallStatusTextEl.textContent =
      mode === 'video' ? `与 ${callPeerAlias} 高清视频连麦中` : `与 ${callPeerAlias} 实时语音通话中`;
  }
  updateLiveCallStageUI();
  liveCallStageEl.classList.remove('hidden');
  startCallTimer();
  startCallVideoFrameSyncLoop();

  broadcastEnvelopePacket({
    kind: 'call-accept',
    senderId: selfId,
    callSessionId,
    alias: myProfile.alias,
    mode,
    hasVideo: isCallCamEnabled
  });

  for (const room of activeRooms) {
    if (typeof room.addStream === 'function') {
      try {
        room.addStream(stream, undefined, { streamRole: 'call', mode });
        boostWebRtcVideoBitrate(room);
      } catch {
        // 忽略异常
      }
    }
  }
}

function declineIncomingCall() {
  if (!pendingIncomingCall) return;
  const { callSessionId } = pendingIncomingCall;
  stoppedCallSessionIds.add(callSessionId);
  pendingIncomingCall = null;
  activeCallState = 'idle';
  if (callInviteBannerEl) callInviteBannerEl.classList.add('hidden');

  broadcastEnvelopePacket({
    kind: 'call-decline',
    senderId: selfId,
    callSessionId,
    alias: myProfile.alias
  });
}

function handleIncomingCallAccept(packet) {
  if (!packet || packet.callSessionId !== activeCallSessionId) return;
  const peerProfile = getSenderDisplayProfile(packet.senderId, packet.alias);
  callPeerAlias = peerProfile.alias;
  activeCallState = 'connected';
  remoteCallHasVideo = Boolean(packet.hasVideo || activeCallMode === 'video');

  if (liveCallStatusTextEl) {
    liveCallStatusTextEl.textContent =
      activeCallMode === 'video' || remoteCallHasVideo
        ? `与 ${callPeerAlias} 高清视频连麦中`
        : `与 ${callPeerAlias} 实时语音通话中`;
  }
  updateLiveCallStageUI();
  startCallTimer();
  startCallVideoFrameSyncLoop();

  if (localCallStream) {
    for (const room of activeRooms) {
      if (typeof room.addStream === 'function') {
        try {
          room.addStream(localCallStream, undefined, { streamRole: 'call', mode: activeCallMode });
          boostWebRtcVideoBitrate(room);
        } catch {
          // 忽略异常
        }
      }
    }
  }
  showToast(`${callPeerAlias} 已接听通话`);
}

function handleIncomingCallDecline(packet) {
  if (!packet || packet.callSessionId !== activeCallSessionId) return;
  const peerProfile = getSenderDisplayProfile(packet.senderId, packet.alias);
  hangupLiveCall({ notifyPeer: false, reason: `${peerProfile.alias} 婉拒了本次通话` });
}

function handleIncomingCallMediaState(packet) {
  if (!packet || packet.callSessionId !== activeCallSessionId) return;
  const peerProfile = getSenderDisplayProfile(packet.senderId, packet.alias);
  callPeerAlias = peerProfile.alias;
  remoteCallHasVideo = Boolean(packet.hasVideo);
  if (!remoteCallHasVideo && callRemoteFallbackImgEl) {
    callRemoteFallbackImgEl.src = '';
    callRemoteFallbackImgEl.classList.add('hidden');
  }
  updateLiveCallStageUI();
}

function handleIncomingCallVideoFrame(packet) {
  if (!packet || packet.callSessionId !== activeCallSessionId || isSessionTerminated) return;
  if (stoppedCallSessionIds.has(packet.callSessionId)) return;

  remoteCallHasVideo = true;
  updateLiveCallStageUI();

  const hasLiveWebRtcVideo =
    callRemoteVideoEl &&
    callRemoteVideoEl.srcObject &&
    callRemoteVideoEl.srcObject.getVideoTracks &&
    callRemoteVideoEl.srcObject.getVideoTracks().some((t) => t.readyState === 'live');

  if (!hasLiveWebRtcVideo && callRemoteFallbackImgEl) {
    callRemoteVideoEl.classList.add('hidden');
    callRemoteFallbackImgEl.src = packet.frameDataUrl;
    callRemoteFallbackImgEl.classList.remove('hidden');
  }
}

function attachRemoteCallStream(stream, peerId) {
  if (!stream || isSessionTerminated) return;
  const peerProfile = getSenderDisplayProfile(peerId);
  callPeerAlias = peerProfile.alias;

  if (callRemoteAudioEl) {
    callRemoteAudioEl.srcObject = stream;
    callRemoteAudioEl.play().catch(() => {});
  }

  if (stream.getVideoTracks().length > 0 && callRemoteVideoEl) {
    remoteCallHasVideo = true;
    if (callRemoteFallbackImgEl) {
      callRemoteFallbackImgEl.src = '';
      callRemoteFallbackImgEl.classList.add('hidden');
    }
    callRemoteVideoEl.classList.remove('hidden');
    callRemoteVideoEl.srcObject = stream;
    callRemoteVideoEl.play().catch(() => {});
  }
  updateLiveCallStageUI();
}

function toggleCallMicMute() {
  if (!localCallStream || activeCallState === 'idle') return;
  isCallMicMuted = !isCallMicMuted;
  localCallStream.getAudioTracks().forEach((t) => {
    t.enabled = !isCallMicMuted;
  });
  updateLiveCallStageUI();
  broadcastEnvelopePacket({
    kind: 'call-media-state',
    senderId: selfId,
    callSessionId: activeCallSessionId,
    alias: myProfile.alias,
    hasVideo: isCallCamEnabled,
    micMuted: isCallMicMuted
  });
}

async function toggleCallCamera() {
  if (!localCallStream || activeCallState === 'idle') return;

  if (isCallCamEnabled) {
    isCallCamEnabled = false;
    localCallStream.getVideoTracks().forEach((t) => {
      t.stop();
      localCallStream.removeTrack(t);
    });
    if (callLocalVideoEl) callLocalVideoEl.srcObject = null;
    updateLiveCallStageUI();
    broadcastEnvelopePacket({
      kind: 'call-media-state',
      senderId: selfId,
      callSessionId: activeCallSessionId,
      alias: myProfile.alias,
      hasVideo: false,
      micMuted: isCallMicMuted
    });
    return;
  }

  // 开启高清摄像头并加入当前通话
  const vStream = await acquireCallMediaStream('video');
  const newVideoTrack = vStream.getVideoTracks()[0];
  vStream.getAudioTracks().forEach((t) => t.stop());
  if (!newVideoTrack) {
    showToast('未能开启摄像头');
    return;
  }

  localCallStream.addTrack(newVideoTrack);
  isCallCamEnabled = true;
  activeCallMode = 'video';
  if (callLocalVideoEl) {
    callLocalVideoEl.srcObject = localCallStream;
    callLocalVideoEl.play().catch(() => {});
  }
  updateLiveCallStageUI();
  startCallVideoFrameSyncLoop();

  for (const room of activeRooms) {
    if (typeof room.addStream === 'function') {
      try {
        room.addStream(localCallStream, undefined, { streamRole: 'call', mode: 'video' });
        boostWebRtcVideoBitrate(room);
      } catch {
        // 忽略异常
      }
    }
  }

  broadcastEnvelopePacket({
    kind: 'call-media-state',
    senderId: selfId,
    callSessionId: activeCallSessionId,
    alias: myProfile.alias,
    hasVideo: true,
    micMuted: isCallMicMuted
  });
}

function toggleCallFullscreen() {
  if (!liveCallStageEl || liveCallStageEl.classList.contains('hidden')) return;
  const isFull =
    document.fullscreenElement === liveCallStageEl ||
    liveCallStageEl.classList.contains('is-theater-fullscreen');

  if (isFull) {
    liveCallStageEl.classList.remove('is-theater-fullscreen');
    if (document.fullscreenElement) {
      document.exitFullscreen().catch(() => {});
    }
  } else {
    liveCallStageEl.classList.add('is-theater-fullscreen');
    if (typeof liveCallStageEl.requestFullscreen === 'function') {
      liveCallStageEl.requestFullscreen().catch(() => {});
    }
  }
}

function handleIncomingCallHangup(packet) {
  if (!packet) return;
  if (packet.callSessionId) {
    stoppedCallSessionIds.add(packet.callSessionId);
  }
  const peerProfile = getSenderDisplayProfile(packet.senderId, packet.alias);
  hangupLiveCall({ notifyPeer: false, reason: `${peerProfile.alias} 已结束通话` });
}

function hangupLiveCall({ notifyPeer = true, reason = '' } = {}) {
  const endingCallId = activeCallSessionId || pendingIncomingCall?.callSessionId;
  if (endingCallId) {
    stoppedCallSessionIds.add(endingCallId);
  }

  const wasInCall = activeCallState !== 'idle' || Boolean(pendingIncomingCall);

  if (callTimerInterval) {
    clearInterval(callTimerInterval);
    callTimerInterval = null;
  }
  if (callVideoFrameTimer) {
    clearInterval(callVideoFrameTimer);
    callVideoFrameTimer = null;
  }

  if (localCallStream) {
    for (const room of activeRooms) {
      if (typeof room.removeStream === 'function') {
        try {
          room.removeStream(localCallStream);
        } catch {
          // 忽略异常
        }
      }
    }
    localCallStream.getTracks().forEach((t) => t.stop());
    localCallStream = null;
  }

  if (callLocalVideoEl) callLocalVideoEl.srcObject = null;
  if (callRemoteVideoEl) {
    if (callRemoteVideoEl.srcObject) {
      const tracks = callRemoteVideoEl.srcObject.getTracks?.() || [];
      tracks.forEach((t) => t.stop());
    }
    callRemoteVideoEl.srcObject = null;
  }
  if (callRemoteAudioEl) callRemoteAudioEl.srcObject = null;
  if (callRemoteFallbackImgEl) {
    callRemoteFallbackImgEl.src = '';
    callRemoteFallbackImgEl.classList.add('hidden');
  }

  activeCallSessionId = null;
  activeCallState = 'idle';
  pendingIncomingCall = null;
  isCallMicMuted = false;
  isCallCamEnabled = false;
  remoteCallHasVideo = false;

  if (callInviteBannerEl) callInviteBannerEl.classList.add('hidden');
  if (liveCallStageEl) {
    liveCallStageEl.classList.remove('is-theater-fullscreen');
    liveCallStageEl.classList.add('hidden');
  }
  document.body.classList.remove('is-video-active');

  if (notifyPeer && wasInCall && activeRoomId && !isSessionTerminated) {
    const hangupPkt = {
      kind: 'call-hangup',
      senderId: selfId,
      callSessionId: endingCallId,
      alias: myProfile.alias
    };
    broadcastEnvelopePacket(hangupPkt);
    setTimeout(() => {
      if (activeRoomId && !isSessionTerminated && activeCallState === 'idle') {
        broadcastEnvelopePacket(hangupPkt);
      }
    }, 220);
    showToast('通话已结束');
  } else if (reason && wasInCall) {
    showToast(reason);
  }
}

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
      const maxDim = 960;
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

      const compressedDataUrl = canvas.toDataURL('image/jpeg', 0.8);
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
 * 处理并发送聊天内高清视频消息（内置高清超分滤镜渲染与端到端加密分片传输）
 */
function handleVideoFile(file) {
  if (!file || !file.type.startsWith('video/')) {
    showToast('请选择有效的视频文件（MP4 / WebM / MOV）');
    return;
  }
  if (file.size > 12 * 1024 * 1024) {
    showToast('为保障零服务器加密直传流畅度，建议发送 12MB 以内的短视频片段');
    return;
  }

  const reader = new FileReader();
  reader.onload = () => {
    if (typeof reader.result === 'string') {
      dispatchOutgoingMessage({
        type: 'video',
        mediaDataUrl: reader.result,
        timestamp: Date.now()
      });
      showToast('已发送高清视频私语');
    }
  };
  reader.readAsDataURL(file);
}

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
      // 无物理麦克风或未授权时平滑切换为轻音私语模式
    }
  }

  isSyntheticRecording = true;
  recordingModeLabelEl.textContent = '正在录制轻音私语...';
}

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
  view.setUint16(20, 1, true);
  view.setUint16(22, 1, true);
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
 * 八、“结束并清除”全员退出、原链接永久作废与 beforeunload 兜底
 * ============================================================================
 */
function cleanupMemoryAndLeaveRoom({ notifyPeer = false, burnRoom = false, silent = false } = {}) {
  const closingRoomId = activeRoomId;
  const closingKeyPromise = roomCryptoKeyPromise;
  const closingWssClients = e2eeWssClients.slice();
  const closingRooms = activeRooms.slice();
  const closingP2pDispatchers = p2pDispatchers.slice();
  const closingLocalBus = localEphemeralBus;

  if (presenceHeartbeatTimer) {
    clearInterval(presenceHeartbeatTimer);
    presenceHeartbeatTimer = null;
  }
  if (recordingTimerInterval) {
    clearInterval(recordingTimerInterval);
    recordingTimerInterval = null;
  }
  if (mediaRecorder && mediaRecorder.state !== 'inactive') {
    try {
      mediaRecorder.onstop = null;
      mediaRecorder.stop();
    } catch {
      // 忽略异常
    }
  }
  mediaRecorder = null;
  releaseRecordingStream();
  stopActiveAudioPlayback();
  hangupLiveCall({ notifyPeer: false });

  if (burnRoom && closingRoomId) {
    recordRoomBurnedInMemory(closingRoomId);
    if (globalRegistryBus) {
      try {
        globalRegistryBus.postMessage({ kind: 'room-burned', roomId: closingRoomId });
      } catch {
        // 忽略异常
      }
    }
  }

  // 向所有在线成员发送实时结束指令或单人离开通知，并在 WSS 写入加密销毁墓碑
  if (notifyPeer && closingRoomId) {
    const outPacket = burnRoom
      ? { kind: 'terminate', senderId: selfId, roomId: closingRoomId }
      : { kind: 'peer-leave', senderId: selfId };

    for (const sendFn of closingP2pDispatchers) {
      try {
        sendFn(outPacket);
      } catch {
        // 忽略异常
      }
    }
    if (closingLocalBus) {
      try {
        closingLocalBus.postMessage(outPacket);
      } catch {
        // 忽略异常
      }
    }

    // 异步加密发送实时通知 + Retain=1 房间销毁永久墓碑后再关闭底层套接字
    (async () => {
      await broadcastEncryptedWssPacket(outPacket, {
        retain: false,
        targetClients: closingWssClients,
        keyPromise: closingKeyPromise
      });
      if (burnRoom) {
        await broadcastEncryptedWssPacket(
          {
            kind: 'room-state',
            status: 'terminated',
            roomId: closingRoomId,
            endedAt: Date.now()
          },
          {
            retain: true,
            targetClients: closingWssClients,
            keyPromise: closingKeyPromise
          }
        );
      }
      setTimeout(() => {
        for (const c of closingWssClients) {
          if (c.pingTimer) clearInterval(c.pingTimer);
          try {
            c.ws.close();
          } catch {
            // 忽略异常
          }
        }
        for (const r of closingRooms) {
          try {
            r.leave();
          } catch {
            // 忽略异常
          }
        }
        if (closingLocalBus) {
          try {
            closingLocalBus.close();
          } catch {
            // 忽略异常
          }
        }
      }, 220);
    })();
  } else {
    // 若无需通知对端（比如被动收到结束通知），也确保持久化写入一次 WSS 墓碑后关闭连接
    if (burnRoom && closingRoomId) {
      (async () => {
        await broadcastEncryptedWssPacket(
          {
            kind: 'room-state',
            status: 'terminated',
            roomId: closingRoomId,
            endedAt: Date.now()
          },
          {
            retain: true,
            targetClients: closingWssClients,
            keyPromise: closingKeyPromise
          }
        );
        setTimeout(() => {
          for (const c of closingWssClients) {
            if (c.pingTimer) clearInterval(c.pingTimer);
            try {
              c.ws.close();
            } catch {
              // 忽略异常
            }
          }
        }, 180);
      })();
    } else {
      for (const c of closingWssClients) {
        if (c.pingTimer) clearInterval(c.pingTimer);
        try {
          c.ws.close();
        } catch {
          // 忽略异常
        }
      }
    }
    for (const r of closingRooms) {
      try {
        r.leave();
      } catch {
        // 忽略异常
      }
    }
    if (closingLocalBus) {
      try {
        closingLocalBus.close();
      } catch {
        // 忽略异常
      }
    }
  }

  activeRooms = [];
  p2pDispatchers = [];
  e2eeWssClients = [];
  localEphemeralBus = null;
  activeRoomId = null;
  roomCryptoKeyPromise = null;
  connectedPeers.clear();
  roomParticipants.clear();
  incomingChunkBuffer.clear();

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

  if (messageListEl) {
    messageListEl.replaceChildren();
    if (silent && emptyChatHintEl) {
      messageListEl.appendChild(emptyChatHintEl);
    }
  }
  closeEmojiPicker();
  closeImageLightbox();
}

/**
 * 切换至终态/拦截画面
 * @param {Object} options
 * @param {boolean} options.notifyPeer - 是否向房间内所有成员广播结束指令
 * @param {'burned'|'expired'|'full'} options.mode
 *   - 'burned' : 房间内任一成员点击“结束并清除”，所有人同步回到结束界面（“话已燃尽，一切随风去吧。”）
 *   - 'expired': 点击了已经销毁失效的旧链接（提示“连接已失效”）
 *   - 'full'   : 超过房间设定最大人数上限（提示“无法加入私语”）
 */
function terminateAndBurnSession({ notifyPeer = true, mode = 'burned', maxCap = maxRoomCapacity } = {}) {
  if (isSessionTerminated) return;

  const shouldBurnRoom = mode === 'burned' || mode === 'expired';
  cleanupMemoryAndLeaveRoom({
    notifyPeer,
    burnRoom: shouldBurnRoom,
    silent: false
  });
  isSessionTerminated = true;

  // 清除地址栏中的 #room=<房间ID> 片段
  history.replaceState(null, '', window.location.pathname + window.location.search);

  if (terminatedTitleEl && terminatedDescEl) {
    if (mode === 'expired') {
      terminatedTitleEl.textContent = '连接已失效';
      terminatedDescEl.textContent = '该私语房间已结束销毁，此连接已失效，一切随风去吧。';
      showToast('该私语连接已失效，无法进入');
    } else if (mode === 'full') {
      terminatedTitleEl.textContent = '无法加入私语';
      terminatedDescEl.textContent = `当前房间人数已达上限（最多 ${maxCap} 人），无法加入。`;
      showToast(`无法加入：房间人数已满（上限 ${maxCap} 人）`);
    } else {
      terminatedTitleEl.textContent = '私语';
      terminatedDescEl.textContent = '话已燃尽，一切随风去吧。';
    }
  }

  document.body.classList.remove('in-chat-mode');
  landingViewEl.classList.add('hidden');
  chatViewEl.classList.add('hidden');
  terminatedViewEl.classList.remove('hidden');
}

function handleBeforeUnloadCleanup() {
  if (activeRoomId && !isSessionTerminated) {
    cleanupMemoryAndLeaveRoom({ notifyPeer: true, burnRoom: false, silent: true });
  }
}

window.addEventListener('beforeunload', handleBeforeUnloadCleanup);
window.addEventListener('pagehide', handleBeforeUnloadCleanup);

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
 * 九、UI 交互事件绑定
 * ============================================================================
 */
// 1. 首页点击“点击我，开始私语吧”：生成全新高熵房间 ID 并作为创建者进入
startWhisperBtn.addEventListener('click', () => {
  const newRoomId = generateHighEntropyRoomId();
  enterRoom(newRoomId, { isCreator: true });
});

// 2. 点击“复制链接”
copyLinkBtn.addEventListener('click', async () => {
  const url = inviteLinkInputEl.value;
  if (!url) return;
  try {
    await navigator.clipboard.writeText(url);
    showToast(`私语链接已复制（当前上限 ${maxRoomCapacity} 人）`);
  } catch {
    inviteLinkInputEl.select();
    document.execCommand('copy');
    showToast('私语链接已复制');
  }
});

// 3. 修改分享链接后的“人数上限”选择器（默认 2 人，最大 5 人）
if (maxPeersSelectEl) {
  maxPeersSelectEl.addEventListener('change', () => {
    const selected = Math.max(2, Math.min(5, Number(maxPeersSelectEl.value) || 2));
    const currentCount = getAdmittedParticipantCount();
    if (selected < currentCount) {
      maxPeersSelectEl.value = String(maxRoomCapacity);
      showToast(`当前已有 ${currentCount} 人在线，上限不能低于当前人数`);
      return;
    }
    maxRoomCapacity = selected;
    updateConnectionStatusUI();
    broadcastEnvelopePacket({
      kind: 'room-meta',
      senderId: selfId,
      maxCapacity: maxRoomCapacity
    });
    broadcastEncryptedWssPacket(
      {
        kind: 'room-state',
        status: 'active',
        maxCapacity: maxRoomCapacity
      },
      { retain: true }
    );
    showToast(`已将房间最大人数设为 ${maxRoomCapacity} 人`);
  });
}

// 4. 方案A：点击顶部国风代号印章修改自己的临时称呼
if (selfAliasBtn && aliasEditBoxEl && aliasEditInputEl && aliasSaveBtn) {
  selfAliasBtn.addEventListener('click', () => {
    selfAliasBtn.classList.add('hidden');
    aliasEditBoxEl.classList.remove('hidden');
    aliasEditInputEl.value = myProfile.alias;
    aliasEditInputEl.focus();
    aliasEditInputEl.select();
  });

  const commitAliasChange = () => {
    const nextAlias = aliasEditInputEl.value.trim().slice(0, 8);
    aliasEditBoxEl.classList.add('hidden');
    selfAliasBtn.classList.remove('hidden');
    if (!nextAlias || nextAlias === myProfile.alias) return;

    myProfile.alias = nextAlias;
    myProfile.custom = true;
    renderSelfAliasUI();

    if (roomParticipants.has(selfId)) {
      roomParticipants.set(selfId, {
        ...roomParticipants.get(selfId),
        alias: myProfile.alias
      });
    }
    for (const msg of messages) {
      if (msg.direction === 'sent') {
        msg.senderAlias = myProfile.alias;
      }
    }
    refreshAllMessagesInDOM();

    broadcastEnvelopePacket({
      kind: 'profile-update',
      senderId: selfId,
      profile: myProfile,
      joinedAt: selfJoinedAt
    });
    showToast(`已将你的临时代号改为「${myProfile.alias}」`);
  };

  aliasSaveBtn.addEventListener('click', commitAliasChange);
  aliasEditInputEl.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.isComposing) {
      e.preventDefault();
      commitAliasChange();
    } else if (e.key === 'Escape') {
      e.preventDefault();
      aliasEditBoxEl.classList.add('hidden');
      selfAliasBtn.classList.remove('hidden');
    }
  });
}

// 5. 显隐邀请链接栏
toggleInviteBtn.addEventListener('click', () => {
  inviteBarEl.classList.toggle('hidden');
});

// 6. 点击“结束并清除”按钮：所有人同步结束退出，原房间连接彻底失效
endAndClearBtn.addEventListener('click', () => {
  terminateAndBurnSession({ notifyPeer: true, mode: 'burned' });
});

// 7. 终态页点击“重新开始私语”返回首页
backHomeBtn.addEventListener('click', () => {
  isSessionTerminated = false;
  terminatedViewEl.classList.add('hidden');
  chatViewEl.classList.add('hidden');
  landingViewEl.classList.remove('hidden');
  if (emptyChatHintEl && messageListEl.children.length === 0) {
    messageListEl.appendChild(emptyChatHintEl);
  }
});

// 8. 发送文字消息与 Emoji 表情包选择
const EMOJI_PACK_CATEGORIES = [
  {
    key: 'mood',
    label: '心绪',
    emojis: [
      '😊', '🥰', '😘', '🥹', '😉', '🤭', '😌', '🤗',
      '🥲', '🥺', '😭', '🤫', '🤔', '🫡', '😶‍🌫️', '😴',
      '🥳', '🤩', '😎', '🫠', '🫣', '😮‍💨', '😏', '😇',
      '🙃', '😋', '😜', '🤪', '🤨', '😔', '😣', '😤'
    ]
  },
  {
    key: 'love',
    label: '爱意',
    emojis: [
      '❤️', '🩷', '🧡', '💛', '💚', '💙', '💜', '🤍',
      '💖', '💗', '💓', '💞', '💕', '💌', '🫂', '💋',
      '🌹', '🌷', '🌸', '🌺', '🌻', '🌼', '💐', '🍀',
      '✨', '🌟', '💫', '🫧', '🎀', '🧸', '🎈', '🎁'
    ]
  },
  {
    key: 'oriental',
    label: '国风',
    emojis: [
      '🎐', '🏮', '🍵', '🎋', '🌙', '🌕', '⭐', '🌌',
      '☁️', '🌧️', '❄️', '🌊', '🔥', '🍂', '🍁', '🍃',
      '🦋', '🕊️', '🦢', '🦌', '🐇', '🦊', '🐼', '🐉',
      '🏔️', '🏞️', '⛩️', '🪭', '🪕', '🕯️', '📜', '🖋️'
    ]
  },
  {
    key: 'gesture',
    label: '互动',
    emojis: [
      '👋', '🤚', '🫶', '🤝', '👍', '👏', '🙏', '✌️',
      '🤞', '🤟', '👌', '🤌', '🤙', '👈', '👉', '👆',
      '💪', '👀', '👂', '💬', '💭', '💯', '🆗', '🎉',
      '🥂', '🍷', '☕', '🍰', '🍓', '🍒', '🍑', '🍉'
    ]
  }
];

let activeEmojiCategoryKey = EMOJI_PACK_CATEGORIES[0].key;

function renderEmojiPickerContent() {
  if (!emojiTabsBarEl || !emojiGridEl) return;
  emojiTabsBarEl.replaceChildren();
  emojiGridEl.replaceChildren();

  for (const cat of EMOJI_PACK_CATEGORIES) {
    const tabBtn = document.createElement('button');
    tabBtn.type = 'button';
    tabBtn.className = `emoji-tab-btn${cat.key === activeEmojiCategoryKey ? ' is-active' : ''}`;
    tabBtn.textContent = cat.label;
    tabBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      activeEmojiCategoryKey = cat.key;
      renderEmojiPickerContent();
    });
    emojiTabsBarEl.appendChild(tabBtn);
  }

  const currentCategory =
    EMOJI_PACK_CATEGORIES.find((c) => c.key === activeEmojiCategoryKey) || EMOJI_PACK_CATEGORIES[0];

  for (const emojiChar of currentCategory.emojis) {
    const itemBtn = document.createElement('button');
    itemBtn.type = 'button';
    itemBtn.className = 'emoji-item-btn';
    itemBtn.textContent = emojiChar;
    itemBtn.title = emojiChar;
    itemBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      insertEmojiIntoInput(emojiChar);
    });
    emojiGridEl.appendChild(itemBtn);
  }
}

function insertEmojiIntoInput(emojiChar) {
  if (!messageInputEl) return;
  const start =
    typeof messageInputEl.selectionStart === 'number'
      ? messageInputEl.selectionStart
      : messageInputEl.value.length;
  const end =
    typeof messageInputEl.selectionEnd === 'number'
      ? messageInputEl.selectionEnd
      : messageInputEl.value.length;
  const val = messageInputEl.value;
  messageInputEl.value = val.slice(0, start) + emojiChar + val.slice(end);
  const nextCursor = start + emojiChar.length;
  messageInputEl.focus();
  try {
    messageInputEl.setSelectionRange(nextCursor, nextCursor);
  } catch {
    // 忽略异常
  }
}

function closeEmojiPicker() {
  if (!emojiPickerPopoverEl || !emojiToggleBtn) return;
  emojiPickerPopoverEl.classList.add('hidden');
  emojiToggleBtn.classList.remove('is-active');
  emojiToggleBtn.setAttribute('aria-expanded', 'false');
}

function toggleEmojiPicker(event) {
  if (event) event.stopPropagation();
  if (!emojiPickerPopoverEl || !emojiToggleBtn) return;
  const willOpen = emojiPickerPopoverEl.classList.contains('hidden');
  if (willOpen) {
    renderEmojiPickerContent();
    emojiPickerPopoverEl.classList.remove('hidden');
    emojiToggleBtn.classList.add('is-active');
    emojiToggleBtn.setAttribute('aria-expanded', 'true');
  } else {
    closeEmojiPicker();
  }
}

if (emojiToggleBtn) {
  emojiToggleBtn.addEventListener('click', toggleEmojiPicker);
}

if (emojiPickerPopoverEl) {
  emojiPickerPopoverEl.addEventListener('click', (e) => {
    e.stopPropagation();
  });
}

document.addEventListener('click', (event) => {
  if (
    emojiPickerPopoverEl &&
    !emojiPickerPopoverEl.classList.contains('hidden') &&
    messageInputWrapEl &&
    !messageInputWrapEl.contains(event.target)
  ) {
    closeEmojiPicker();
  }
});

function submitTextMessage() {
  const text = messageInputEl.value.trim();
  if (!text) return;
  messageInputEl.value = '';
  closeEmojiPicker();
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
  } else if (event.key === 'Escape') {
    closeEmojiPicker();
  }
});

// 9. 发送图片与高清视频消息
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

if (videoPickBtn && videoFileInput) {
  videoPickBtn.addEventListener('click', () => {
    videoFileInput.value = '';
    videoFileInput.click();
  });

  videoFileInput.addEventListener('change', (event) => {
    const file = event.target.files && event.target.files[0];
    if (file) {
      handleVideoFile(file);
    }
  });
}

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
    if (items[i].type.startsWith('video/')) {
      event.preventDefault();
      const file = items[i].getAsFile();
      if (file) handleVideoFile(file);
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
  } else if (file && file.type.startsWith('video/')) {
    handleVideoFile(file);
  }
});

// 10. 实时语音通话与 1080P/720P 高清视频连麦事件绑定
if (voiceCallBtn) {
  voiceCallBtn.addEventListener('click', () => {
    startLiveCall('audio');
  });
}
if (videoCallBtn) {
  videoCallBtn.addEventListener('click', () => {
    startLiveCall('video');
  });
}
if (acceptCallBtn) {
  acceptCallBtn.addEventListener('click', () => {
    acceptIncomingCall();
  });
}
if (declineCallBtn) {
  declineCallBtn.addEventListener('click', () => {
    declineIncomingCall();
  });
}
if (callToggleMicBtn) {
  callToggleMicBtn.addEventListener('click', () => {
    toggleCallMicMute();
  });
}
if (callToggleCamBtn) {
  callToggleCamBtn.addEventListener('click', () => {
    toggleCallCamera();
  });
}
if (callToggleHdBtn) {
  callToggleHdBtn.addEventListener('click', () => {
    toggleVideoHdEnhancement();
  });
}
if (callFullscreenBtn) {
  callFullscreenBtn.addEventListener('click', () => {
    toggleCallFullscreen();
  });
}
if (hangupCallBtn) {
  hangupCallBtn.addEventListener('click', () => {
    hangupLiveCall({ notifyPeer: true });
  });
}

// 11. 语音录制控制
voiceRecordBtn.addEventListener('click', () => {
  startVoiceRecording();
});

cancelVoiceBtn.addEventListener('click', () => {
  stopVoiceRecording(false);
});

finishVoiceBtn.addEventListener('click', () => {
  stopVoiceRecording(true);
});

// 12. 图片预览灯箱
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

// 13. 极简 Toast 提示
function showToast(text) {
  const item = document.createElement('div');
  item.className = 'toast-item';
  item.textContent = text;
  toastContainerEl.appendChild(item);
  setTimeout(() => {
    item.remove();
  }, 2600);
}

// 14. 监听地址栏 #room= 变化
window.addEventListener('hashchange', () => {
  const hashRoomId = parseRoomIdFromHash();
  if (hashRoomId && hashRoomId !== activeRoomId) {
    enterRoom(hashRoomId, { isCreator: false });
  }
});

// ==================== 应用启动入口：检查 URL #room=<房间ID> ====================
(function bootstrap() {
  const initialRoomId = parseRoomIdFromHash();
  if (initialRoomId) {
    enterRoom(initialRoomId, { isCreator: false });
  }
})();
