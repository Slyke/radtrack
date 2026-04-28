import { WebSocketServer } from 'ws';

const liveUpdateWebSocketPath = '/api/map/live-updates/ws';
const maxHistoryEntries = 5000;
const historyTtlMs = 6 * 60 * 60 * 1000;

const normalizeCursor = ({ value }) => {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  const parsed = Number.parseInt(String(value), 10);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : null;
};

const normalizePointValue = ({ value }) => (Number.isFinite(Number(value)) ? Number(value) : null);

const buildPointEvent = ({
  datasetId,
  datalogId,
  reading
}) => ({
  id: String(reading?.id ?? ''),
  datasetId: String(datasetId ?? ''),
  datalogId: String(datalogId ?? ''),
  occurredAt: typeof reading?.occurredAt === 'string' ? reading.occurredAt : null,
  receivedAt: typeof reading?.receivedAt === 'string' ? reading.receivedAt : null,
  latitude: normalizePointValue({ value: reading?.latitude }),
  longitude: normalizePointValue({ value: reading?.longitude }),
  altitudeMeters: normalizePointValue({ value: reading?.altitudeMeters }),
  accuracy: normalizePointValue({ value: reading?.accuracy }),
  measurements: reading?.measurements && typeof reading.measurements === 'object' && !Array.isArray(reading.measurements)
    ? reading.measurements
    : {}
});

const createSnapshot = ({ currentCursor, currentPublishedAt }) => ({
  currentCursor,
  currentPublishedAt: currentPublishedAt ?? new Date().toISOString()
});

const createStatusPayload = ({
  currentCursor,
  currentPublishedAt,
  latestMatch = null,
  updateCount
}) => ({
  hasUpdates: Boolean(latestMatch),
  updateCount,
  latestCursor: latestMatch?.cursor ?? currentCursor,
  latestPublishedAt: latestMatch?.publishedAt ?? currentPublishedAt,
  currentCursor,
  currentPublishedAt
});

const sendSocketJson = ({ socket, body }) => {
  if (socket.readyState !== 1) {
    return;
  }

  socket.send(JSON.stringify(body));
};

export const createLiveUpdateService = ({
  authService,
  logger,
  queryService,
  server
}) => {
  let currentCursor = 0;
  let currentPublishedAt = null;
  const history = [];
  const clients = new Map();
  const wsServer = new WebSocketServer({
    server,
    path: liveUpdateWebSocketPath
  });

  const pruneHistory = () => {
    const cutoff = Date.now() - historyTtlMs;

    while (history.length > maxHistoryEntries) {
      history.shift();
    }

    while (history[0] && history[0].publishedAtMillis < cutoff) {
      history.shift();
    }
  };

  const getSnapshot = () => createSnapshot({
    currentCursor,
    currentPublishedAt
  });

  const getMatchingStatus = ({
    matcher,
    sinceCursor
  }) => {
    const snapshot = getSnapshot();
    if (sinceCursor === null) {
      return createStatusPayload({
        currentCursor: snapshot.currentCursor,
        currentPublishedAt: snapshot.currentPublishedAt,
        latestMatch: null,
        updateCount: 0
      });
    }

    const matchingEntries = history.filter((entry) => (
      entry.cursor > sinceCursor
      && matcher.matches({ point: entry.point })
    ));

    return createStatusPayload({
      currentCursor: snapshot.currentCursor,
      currentPublishedAt: snapshot.currentPublishedAt,
      latestMatch: matchingEntries[matchingEntries.length - 1] ?? null,
      updateCount: matchingEntries.length
    });
  };

  const handleSubscribe = async ({
    client,
    payload
  }) => {
    if (!client.auth) {
      sendSocketJson({
        socket: client.socket,
        body: {
          type: 'error',
          reason: 'Authentication is still initializing.'
        }
      });
      return;
    }

    const subscriptionVersion = client.subscriptionVersion + 1;
    client.subscriptionVersion = subscriptionVersion;

    try {
      const matcher = await queryService.createLiveUpdateMatcher({
        user: client.auth.user,
        input: payload?.filters && typeof payload.filters === 'object' && !Array.isArray(payload.filters)
          ? payload.filters
          : {},
        correlationId: null
      });
      if (client.subscriptionVersion !== subscriptionVersion || client.socket.readyState !== 1) {
        return;
      }

      const rawSinceCursor = normalizeCursor({ value: payload?.sinceCursor });
      const effectiveSinceCursor = rawSinceCursor ?? getSnapshot().currentCursor;
      client.subscription = {
        matcher,
        sinceCursor: effectiveSinceCursor
      };

      const snapshot = getSnapshot();
      sendSocketJson({
        socket: client.socket,
        body: {
          type: 'subscribed',
          currentCursor: snapshot.currentCursor,
          currentPublishedAt: snapshot.currentPublishedAt
        }
      });

      const status = getMatchingStatus({
        matcher,
        sinceCursor: effectiveSinceCursor
      });
      if (status.hasUpdates) {
        sendSocketJson({
          socket: client.socket,
          body: {
            type: 'updates-available',
            latestCursor: status.latestCursor,
            latestPublishedAt: status.latestPublishedAt,
            updateCount: status.updateCount
          }
        });
      }
    } catch (error) {
      if (client.subscriptionVersion !== subscriptionVersion || client.socket.readyState !== 1) {
        return;
      }

      logger.warn({
        caller: 'liveUpdates::subscribe',
        message: 'Failed preparing live update subscription.',
        correlationId: null,
        errorKey: 'LIVE_UPDATES_SUBSCRIBE_FAILED',
        context: {
          userId: client.auth.user.id,
          reason: error instanceof Error ? error.message : 'Unknown error'
        }
      });
      sendSocketJson({
        socket: client.socket,
        body: {
          type: 'error',
          reason: error instanceof Error ? error.message : 'Failed preparing live updates subscription.'
        }
      });
    }
  };

  wsServer.on('connection', (socket, req) => {
    const client = {
      auth: null,
      socket,
      subscription: null,
      subscriptionVersion: 0
    };
    clients.set(socket, client);

    void (async () => {
      try {
        req.sourceIp = req.socket.remoteAddress ?? '';
        client.auth = await authService.getAuthenticatedRequest({
          req,
          correlationId: null
        });
        if (!client.auth) {
          socket.close(4401, 'Authentication required');
          return;
        }

        const snapshot = getSnapshot();
        sendSocketJson({
          socket,
          body: {
            type: 'ready',
            currentCursor: snapshot.currentCursor,
            currentPublishedAt: snapshot.currentPublishedAt
          }
        });
      } catch (error) {
        logger.warn({
          caller: 'liveUpdates::connection',
          message: 'Failed authenticating live update websocket.',
          correlationId: null,
          errorKey: 'LIVE_UPDATES_AUTH_FAILED',
          context: {
            reason: error instanceof Error ? error.message : 'Unknown error'
          }
        });
        socket.close(1011, 'Authentication failed');
      }
    })();

    socket.on('message', (rawMessage) => {
      let payload = null;
      try {
        payload = JSON.parse(rawMessage.toString());
      } catch {
        sendSocketJson({
          socket,
          body: {
            type: 'error',
            reason: 'Live update messages must be valid JSON.'
          }
        });
        return;
      }

      if (payload?.type !== 'subscribe') {
        sendSocketJson({
          socket,
          body: {
            type: 'error',
            reason: 'Unsupported live update message.'
          }
        });
        return;
      }

      void handleSubscribe({
        client,
        payload
      });
    });

    socket.on('close', () => {
      clients.delete(socket);
    });
  });

  const publishPointUpdate = ({
    datasetId,
    datalogId,
    reading
  }) => {
    const point = buildPointEvent({
      datasetId,
      datalogId,
      reading
    });
    if (!point.id || !point.datasetId || !point.datalogId) {
      return;
    }

    const publishedAt = new Date().toISOString();
    const entry = {
      cursor: currentCursor + 1,
      point,
      publishedAt,
      publishedAtMillis: Date.parse(publishedAt)
    };
    currentCursor = entry.cursor;
    currentPublishedAt = publishedAt;
    history.push(entry);
    pruneHistory();

    for (const client of clients.values()) {
      if (!client.subscription || client.socket.readyState !== 1) {
        continue;
      }

      if (entry.cursor <= client.subscription.sinceCursor) {
        continue;
      }

      if (!client.subscription.matcher.matches({ point: entry.point })) {
        continue;
      }

      sendSocketJson({
        socket: client.socket,
        body: {
          type: 'updates-available',
          latestCursor: entry.cursor,
          latestPublishedAt: entry.publishedAt,
          updateCount: 1
        }
      });
    }
  };

  const getUpdatesStatus = async ({
    user,
    input,
    sinceCursor,
    correlationId = null
  }) => {
    const matcher = await queryService.createLiveUpdateMatcher({
      user,
      input,
      correlationId
    });
    return getMatchingStatus({
      matcher,
      sinceCursor: normalizeCursor({ value: sinceCursor })
    });
  };

  const close = async () => {
    for (const client of clients.values()) {
      client.socket.close(1001, 'Server shutting down');
    }

    await new Promise((resolve) => {
      wsServer.close(() => resolve());
    });
  };

  return {
    close,
    getSnapshot,
    getUpdatesStatus,
    publishPointUpdate
  };
};
