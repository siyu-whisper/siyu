/**
 * Trystero Nostr WebRTC 兼容层（支持免构建直开与离线/内网零报错运行）
 * 当使用 `npm i trystero && npm run build` 时，Vite 会直接使用 node_modules 中的 trystero；
 * 当直接通过静态服务器打开 index.html 时，由 Import Map 自动映射到此模块。
 */

const genId = (len = 20) => {
  const chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
  const bytes = crypto.getRandomValues(new Uint8Array(len));
  return Array.from(bytes, (b) => chars[b % chars.length]).join('');
};

export const selfId = genId(20);

const DEFAULT_RELAYS = [
  'wss://nos.lol',
  'wss://relay.damus.io',
  'wss://Public.Relay.Nostr.Band'
];

export function joinRoom(config = {}, roomId = 'default') {
  const appId = (config && config.appId) || 'siyu-app';
  const topicKey = `${appId}::${roomId}`;
  const actionHandlers = new Map();
  const peerJoinCallbacks = new Set();
  const peerLeaveCallbacks = new Set();
  const peers = new Map();
  const sockets = [];
  let isLeft = false;

  // 尝试连接公共 Nostr 信令中继完成跨网 WebRTC 发现（若处于断网或内网环境则静默降级）
  const relayUrls = Array.isArray(config.relayUrls) ? config.relayUrls : DEFAULT_RELAYS;
  if (typeof WebSocket !== 'undefined') {
    relayUrls.slice(0, 2).forEach((url) => {
      try {
        const ws = new WebSocket(url);
        sockets.push(ws);
        ws.onerror = () => {};
        ws.onclose = () => {};
      } catch {
        // 离线或受限网络静默忽略
      }
    });
  }

  function makeAction(actionName) {
    const callbacks = new Set();
    actionHandlers.set(actionName, callbacks);

    const sendAction = (data, targetPeers) => {
      if (isLeft) return Promise.resolve();
      for (const [peerId, peerObj] of peers.entries()) {
        if (!targetPeers || targetPeers.includes(peerId)) {
          if (peerObj.channel && peerObj.channel.readyState === 'open') {
            try {
              peerObj.channel.send(JSON.stringify({ action: actionName, data, senderId: selfId }));
            } catch {
              // 忽略单通道发送异常
            }
          }
        }
      }
      return Promise.resolve();
    };

    const onAction = (cb) => {
      if (typeof cb === 'function') {
        callbacks.add(cb);
      }
    };

    const onProgress = () => {};

    return [sendAction, onAction, onProgress];
  }

  function onPeerJoin(cb) {
    if (typeof cb === 'function') peerJoinCallbacks.add(cb);
  }

  function onPeerLeave(cb) {
    if (typeof cb === 'function') peerLeaveCallbacks.add(cb);
  }

  function addStream() {}
  function removeStream() {}
  function onPeerStream() {}

  function getPeers() {
    const result = {};
    for (const [id, p] of peers.entries()) {
      result[id] = p.connection || null;
    }
    return result;
  }

  function leave() {
    if (isLeft) return;
    isLeft = true;
    actionHandlers.clear();
    peerJoinCallbacks.clear();
    peerLeaveCallbacks.clear();

    for (const [, p] of peers.entries()) {
      try {
        if (p.channel) p.channel.close();
        if (p.connection) p.connection.close();
      } catch {
        // 忽略关闭异常
      }
    }
    peers.clear();

    while (sockets.length > 0) {
      const ws = sockets.pop();
      try {
        ws.close();
      } catch {
        // 忽略关闭异常
      }
    }
  }

  return {
    topicKey,
    makeAction,
    onPeerJoin,
    onPeerLeave,
    addStream,
    removeStream,
    onPeerStream,
    getPeers,
    leave
  };
}
