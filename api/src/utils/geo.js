const earthRadiusMeters = 6378137;
const mercatorMaxLatitude = 85.05112878;

const toRadians = (value) => (value * Math.PI) / 180;
const toDegrees = (value) => (value * 180) / Math.PI;

export const metersPerDegreeLatitude = () => 111320;

export const metersPerDegreeLongitude = ({ latitude }) => Math.cos(toRadians(latitude)) * 111320;

const roundMetricValue = ({ value, decimals }) => {
  const scale = 10 ** decimals;
  return Math.round(value * scale) / scale;
};

export const pointInCircle = ({ point, circle }) => {
  const dx = (point.longitude - circle.center.longitude) * metersPerDegreeLongitude({ latitude: circle.center.latitude });
  const dy = (point.latitude - circle.center.latitude) * metersPerDegreeLatitude();
  return Math.sqrt((dx ** 2) + (dy ** 2)) <= circle.radiusMeters;
};

export const pointInPolygon = ({ point, polygon }) => {
  let inside = false;

  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i, i += 1) {
    const xi = polygon[i].longitude;
    const yi = polygon[i].latitude;
    const xj = polygon[j].longitude;
    const yj = polygon[j].latitude;

    const intersects = ((yi > point.latitude) !== (yj > point.latitude))
      && (
        point.longitude
        < (((xj - xi) * (point.latitude - yi)) / ((yj - yi) || Number.EPSILON)) + xi
      );

    if (intersects) {
      inside = !inside;
    }
  }

  return inside;
};

const clampLatitude = ({ latitude }) => Math.max(
  -mercatorMaxLatitude,
  Math.min(mercatorMaxLatitude, latitude)
);

const toProjectedMeters = ({ latitude, longitude }) => {
  const safeLatitude = clampLatitude({ latitude });

  return {
    x: earthRadiusMeters * toRadians(longitude),
    y: earthRadiusMeters * Math.log(Math.tan((Math.PI / 4) + (toRadians(safeLatitude) / 2)))
  };
};

const fromProjectedMeters = ({ x, y }) => ({
  latitude: toDegrees((2 * Math.atan(Math.exp(y / earthRadiusMeters))) - (Math.PI / 2)),
  longitude: toDegrees(x / earthRadiusMeters)
});

export const buildAggregateCell = ({ point, shape, cellSizeMeters }) => {
  const meters = toProjectedMeters({
    latitude: point.latitude,
    longitude: point.longitude
  });

  if (shape === 'hexagon') {
    const size = cellSizeMeters;
    const q = ((Math.sqrt(3) / 3) * meters.x - (1 / 3) * meters.y) / size;
    const r = ((2 / 3) * meters.y) / size;
    const s = -q - r;
    let roundedQ = Math.round(q);
    let roundedR = Math.round(r);
    let roundedS = Math.round(s);

    const qDiff = Math.abs(roundedQ - q);
    const rDiff = Math.abs(roundedR - r);
    const sDiff = Math.abs(roundedS - s);

    if (qDiff > rDiff && qDiff > sDiff) {
      roundedQ = -roundedR - roundedS;
    } else if (rDiff > sDiff) {
      roundedR = -roundedQ - roundedS;
    } else {
      roundedS = -roundedQ - roundedR;
    }

    const center = fromProjectedMeters({
      x: size * Math.sqrt(3) * (roundedQ + (roundedR / 2)),
      y: size * 1.5 * roundedR
    });

    return {
      id: `hex:${roundedQ}:${roundedR}`,
      center,
      radiusMeters: size
    };
  }

  const step = cellSizeMeters;
  const cellX = Math.round(meters.x / step);
  const cellY = Math.round(meters.y / step);
  const center = fromProjectedMeters({
    x: cellX * step,
    y: cellY * step
  });

  return {
    id: `${shape}:${cellX}:${cellY}`,
    center,
    radiusMeters: shape === 'circle' ? cellSizeMeters / 2 : cellSizeMeters
  };
};

export const computeAggregateStats = ({ values, modeBucketDecimals }) => {
  if (!values.length) {
    return {
      min: null,
      max: null,
      mean: null,
      median: null,
      mode: null,
      count: 0
    };
  }

  const sorted = [...values].sort((left, right) => left - right);
  const sum = sorted.reduce((total, value) => total + value, 0);
  const modeCounts = new Map();

  for (const value of sorted) {
    const bucket = roundMetricValue({ value, decimals: modeBucketDecimals });
    modeCounts.set(bucket, (modeCounts.get(bucket) ?? 0) + 1);
  }

  let mode = null;
  let modeCount = -1;
  for (const [bucket, count] of modeCounts.entries()) {
    if (count > modeCount) {
      mode = bucket;
      modeCount = count;
    }
  }

  const midpoint = Math.floor(sorted.length / 2);
  const median = sorted.length % 2 === 0
    ? (sorted[midpoint - 1] + sorted[midpoint]) / 2
    : sorted[midpoint];

  return {
    min: sorted[0],
    max: sorted[sorted.length - 1],
    mean: sum / sorted.length,
    median,
    mode,
    count: sorted.length
  };
};
