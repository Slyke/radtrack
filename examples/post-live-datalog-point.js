const BASE_URL = process.env.BASE_URL ?? 'http://localhost:4096';
const INGEST_DATALOG_ID = process.env.INGEST_DATALOG_ID ?? '4793100531423fd0';
const API_KEY = process.env.RADTRACK_API_KEY ?? 'rtk-88916fdd3aa3d4f26278';

const POSITION = {
  latitude: 49.2827,
  longitude: -123.1207,
  altitudeMeters: 18.5
};

const FUZZ = {
  latitude: 0.0025,
  longitude: 0.0025,
  altitudeMeters: 6,
  accuracy: [2, 8],
  occurredAtSecondsAgo: [0, 300]
};

const MEASUREMENTS = {
  doseRate: [0.08, 0.16],
  countRate: [80, 110]
};

const DEVICE = {
  id: 'device-001',
  name: 'Pocket Sensor',
  type: 'Generic Sensor',
  calibration: 'factory-2026-01',
  firmwareVersion: '1.2.3'
};

const COMPONENTS = {
  custom: '{"trip":"harbour"}'
};

const EXTRA = {
  gpsFix: '3d',
  satelliteCount: [10, 16]
};

const COMMENT = 'Delayed upload after reconnect';
const SOURCE_READING_PREFIX = 'reading';

const randomBetween = ([min, max], decimals = 0) => Number((min + Math.random() * (max - min)).toFixed(decimals));
const jitter = (value, amount, decimals) => Number((value + (Math.random() * 2 - 1) * amount).toFixed(decimals));

if (!API_KEY || API_KEY === '<generated-key>') {
  throw new Error('Set RADTRACK_API_KEY before running this script.');
}

const occurredAt = new Date(Date.now() - randomBetween(FUZZ.occurredAtSecondsAgo) * 1000).toISOString();
const sourceReadingId = `${SOURCE_READING_PREFIX}-${Date.now()}-${randomBetween([1000, 9999])}`;

const payload = {
  occurredAt,
  latitude: jitter(POSITION.latitude, FUZZ.latitude, 6),
  longitude: jitter(POSITION.longitude, FUZZ.longitude, 6),
  altitudeMeters: jitter(POSITION.altitudeMeters, FUZZ.altitudeMeters, 1),
  accuracy: randomBetween(FUZZ.accuracy, 1),
  measurements: {
    doseRate: randomBetween(MEASUREMENTS.doseRate, 3),
    countRate: randomBetween(MEASUREMENTS.countRate)
  },
  deviceId: DEVICE.id,
  components: {
    deviceName: DEVICE.name,
    deviceType: DEVICE.type,
    deviceCalibration: DEVICE.calibration,
    firmwareVersion: DEVICE.firmwareVersion,
    sourceReadingId,
    comment: COMMENT,
    custom: COMPONENTS.custom
  },
  extra: {
    gpsFix: EXTRA.gpsFix,
    satelliteCount: randomBetween(EXTRA.satelliteCount)
  }
};

const response = await fetch(`${BASE_URL}/api/ingest/datalogs/${INGEST_DATALOG_ID}/points`, {
  method: 'POST',
  headers: {
    'content-type': 'application/json',
    'x-radtrack-api-key': API_KEY
  },
  body: JSON.stringify(payload)
});

const bodyText = await response.text();
const body = (() => {
  try {
    return JSON.parse(bodyText);
  } catch {
    return bodyText;
  }
})();

if (!response.ok) {
  process.exitCode = 1;
}

console.log(JSON.stringify({ status: response.status, payload, response: body }, null, 2));
