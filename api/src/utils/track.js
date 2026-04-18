import yauzl from 'yauzl';
import { Buffer } from 'node:buffer';
import { createAppError } from '../lib/errors.js';
import { sha256Hex } from './ids.js';

const filetimeEpochOffset = 116444736000000000n;

const parseNumber = ({ value }) => {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const parseRawTimeTextUtc = ({ value }) => {
  if (!value || typeof value !== 'string') {
    return null;
  }

  const iso = value.trim().replace(' ', 'T');
  const parsed = new Date(`${iso}Z`);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  return parsed.toISOString();
};

const parseWindowsFileTime = ({ value }) => {
  if (!value || !/^\d+$/.test(String(value))) {
    return null;
  }

  try {
    const ticks = BigInt(String(value));
    const unixMs = Number((ticks - filetimeEpochOffset) / 10000n);
    const parsed = new Date(unixMs);
    if (Number.isNaN(parsed.getTime())) {
      return null;
    }

    return parsed.toISOString();
  } catch {
    return null;
  }
};

const parseDeviceIdentifier = ({ raw }) => {
  if (!raw || typeof raw !== 'string') {
    return {
      deviceIdentifierRaw: null,
      deviceModel: null,
      deviceSerial: null
    };
  }

  const trimmed = raw.trim();
  if (!trimmed) {
    return {
      deviceIdentifierRaw: null,
      deviceModel: null,
      deviceSerial: null
    };
  }

  const match = trimmed.match(/^(.*)-([^-]+)$/);
  return {
    deviceIdentifierRaw: trimmed,
    deviceModel: match ? match[1] : null,
    deviceSerial: match ? match[2] : null
  };
};

const splitTsvLine = ({ line }) => String(line ?? '').split('\t');

const warningTypeReasonMap = {
  timestamp_mismatch: 'Timestamp and text time did not agree.',
  duplicate_row: 'Duplicate readings were detected.'
};

export const parseTrackBuffer = ({ buffer, fileName, correlationId = null }) => {
  const text = buffer.toString('utf8').replace(/^\uFEFF/, '');
  const rawLines = text.split(/\r?\n/).filter((line, index, array) => !(index === array.length - 1 && line === ''));

  if (rawLines.length < 2) {
    throw createAppError({
      caller: 'track::parseTrackBuffer',
      reason: 'Track payload must contain at least two lines.',
      errorKey: 'IMPORT_PARSE_FAILED',
      correlationId,
      context: { fileName }
    });
  }

  const rawHeaderLine = rawLines[0];
  const headerLine = rawLines[1];
  const headerTokens = splitTsvLine({ line: rawHeaderLine });
  const columns = splitTsvLine({ line: headerLine });
  const trackNameToken = String(headerTokens[0] ?? '').trim();
  const trackName = trackNameToken.startsWith('Track:')
    ? trackNameToken.slice('Track:'.length).trim()
    : trackNameToken || fileName;
  const device = parseDeviceIdentifier({ raw: headerTokens[1] ?? '' });
  const headerMetadata = {
    rawTokens: headerTokens,
    unknownTokens: headerTokens.slice(2)
  };

  let warningCount = 0;
  let errorCount = 0;
  let skippedRowCount = 0;
  let validRowCount = 0;
  let startedAt = null;
  let endedAt = null;
  const warnings = [];
  const warningTypeCounts = new Map();
  const rows = [];
  const rowDedup = new Set();
  const incrementWarningTypeCount = (type) => {
    warningTypeCounts.set(type, (warningTypeCounts.get(type) ?? 0) + 1);
  };

  for (let rowIndex = 2; rowIndex < rawLines.length; rowIndex += 1) {
    const rawLine = rawLines[rowIndex];
    if (!rawLine.trim()) {
      skippedRowCount += 1;
      continue;
    }

    const tokens = splitTsvLine({ line: rawLine });
    if (tokens.length < columns.length - 1) {
      errorCount += 1;
      skippedRowCount += 1;
      warnings.push({
        rowNumber: rowIndex + 1,
        type: 'malformed_row',
        reason: 'Row has fewer columns than expected.'
      });
      continue;
    }

    const record = Object.fromEntries(columns.map((column, index) => [column, tokens[index] ?? '']));
    const extraColumns = Object.fromEntries(tokens.slice(columns.length).map((value, index) => [`extra_${index + 1}`, value]));
    const occurredAtFromFiletime = parseWindowsFileTime({ value: record.Timestamp });
    const occurredAtFromText = parseRawTimeTextUtc({ value: record.Time });
    const warningFlags = [];

    let occurredAt = occurredAtFromFiletime ?? occurredAtFromText;
    if (
      occurredAtFromFiletime
      && occurredAtFromText
      && occurredAtFromFiletime !== occurredAtFromText
    ) {
      warningFlags.push('timestamp_mismatch');
      warningCount += 1;
      incrementWarningTypeCount('timestamp_mismatch');
    }

    const latitude = parseNumber({ value: record.Latitude });
    const longitude = parseNumber({ value: record.Longitude });
    if (latitude === null || longitude === null) {
      errorCount += 1;
      skippedRowCount += 1;
      warnings.push({
        rowNumber: rowIndex + 1,
        type: 'invalid_coordinates',
        reason: 'Latitude or longitude is missing or invalid.'
      });
      continue;
    }

    const rowKey = sha256Hex({ value: `${record.Timestamp}\u0000${record.Time}\u0000${latitude}\u0000${longitude}\u0000${record.DoseRate}\u0000${record.CountRate}\u0000${record.Comment ?? ''}` });
    if (rowDedup.has(rowKey)) {
      warningFlags.push('duplicate_row');
      warningCount += 1;
      incrementWarningTypeCount('duplicate_row');
    }
    rowDedup.add(rowKey);

    if (!startedAt || (occurredAt && occurredAt < startedAt)) {
      startedAt = occurredAt;
    }
    if (!endedAt || (occurredAt && occurredAt > endedAt)) {
      endedAt = occurredAt;
    }

    rows.push({
      rowNumber: rowIndex + 1,
      rawTimestamp: record.Timestamp || null,
      parsedTimeText: record.Time || null,
      occurredAt,
      latitude,
      longitude,
      accuracy: parseNumber({ value: record.Accuracy }),
      doseRate: parseNumber({ value: record.DoseRate }),
      countRate: parseNumber({ value: record.CountRate }),
      comment: record.Comment ?? '',
      warningFlags,
      extraJson: extraColumns
    });
    validRowCount += 1;
  }

  return {
    trackName,
    rawHeaderLine,
    rawColumns: columns,
    headerMetadata,
    device,
    rowCount: Math.max(rawLines.length - 2, 0),
    validRowCount,
    warningCount,
    errorCount,
    skippedRowCount,
    startedAt,
    endedAt,
    warningBreakdown: Array.from(warningTypeCounts.entries()).map(([type, count]) => ({
      type,
      count,
      reason: warningTypeReasonMap[type] ?? type
    })),
    warnings,
    rows
  };
};

const openZipBuffer = ({ buffer }) => new Promise((resolve, reject) => {
  yauzl.fromBuffer(buffer, { lazyEntries: true, decodeStrings: true }, (error, zipFile) => {
    if (error) {
      reject(error);
      return;
    }

    resolve(zipFile);
  });
});

const readEntryBuffer = ({ zipFile, entry }) => new Promise((resolve, reject) => {
  zipFile.openReadStream(entry, (error, stream) => {
    if (error) {
      reject(error);
      return;
    }

    const chunks = [];
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('end', () => resolve(Buffer.concat(chunks)));
    stream.on('error', reject);
  });
});

const isSupportedTrackName = ({ fileName }) => {
  const normalized = fileName.toLowerCase();
  return normalized.endsWith('.rctrk');
};

export const extractSupportedArchiveEntries = async ({ buffer, correlationId = null }) => {
  const zipFile = await openZipBuffer({ buffer });
  const entries = [];

  return await new Promise((resolve, reject) => {
    zipFile.readEntry();

    zipFile.on('entry', async (entry) => {
      try {
        const fileName = entry.fileName;
        if (fileName.includes('..') || fileName.startsWith('/') || fileName.startsWith('\\')) {
          zipFile.close();
          reject(createAppError({
            caller: 'track::extractSupportedArchiveEntries',
            reason: 'Archive entry path is unsafe.',
            errorKey: 'IMPORT_ARCHIVE_INVALID',
            correlationId,
            context: { fileName }
          }));
          return;
        }

        const isDirectory = /\/$/.test(fileName);
        if (isDirectory) {
          zipFile.readEntry();
          return;
        }

        const entryBuffer = await readEntryBuffer({ zipFile, entry });
        if (isSupportedTrackName({ fileName })) {
          entries.push({
            fileName,
            buffer: entryBuffer,
            checksum: sha256Hex({ value: entryBuffer }),
            sizeBytes: entryBuffer.length
          });
        }

        zipFile.readEntry();
      } catch (error) {
        zipFile.close();
        reject(error);
      }
    });

    zipFile.once('end', () => resolve(entries));
    zipFile.once('error', reject);
  });
};

export const detectImportKind = ({ fileName, buffer }) => {
  const lowerName = String(fileName ?? '').toLowerCase();
  const zipSignature = buffer.length >= 4
    && buffer[0] === 0x50
    && buffer[1] === 0x4b
    && (buffer[2] === 0x03 || buffer[2] === 0x05 || buffer[2] === 0x07)
    && (buffer[3] === 0x04 || buffer[3] === 0x06 || buffer[3] === 0x08);

  if (zipSignature || lowerName.endsWith('.zip') || lowerName.endsWith('.zrctrk')) {
    return 'archive';
  }

  return 'track_file';
};
