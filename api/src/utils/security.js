import { randomBytes, scryptSync, timingSafeEqual } from 'node:crypto';

export const normalizeUsername = ({ username }) => String(username ?? '').trim().toLowerCase();

export const hashPassword = ({ password }) => {
  const salt = randomBytes(16).toString('hex');
  const hash = scryptSync(password, salt, 64).toString('hex');
  return `scrypt$${salt}$${hash}`;
};

export const verifyPassword = ({ password, storedHash }) => {
  if (!storedHash || typeof storedHash !== 'string' || !storedHash.startsWith('scrypt$')) {
    return false;
  }

  const [, salt, hash] = storedHash.split('$');
  if (!salt || !hash) {
    return false;
  }

  const derived = scryptSync(password, salt, 64);
  const target = Buffer.from(hash, 'hex');
  if (derived.length !== target.length) {
    return false;
  }

  return timingSafeEqual(derived, target);
};

export const safeCompare = ({ left, right }) => {
  if (typeof left !== 'string' || typeof right !== 'string') {
    return false;
  }

  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);
  if (leftBuffer.length !== rightBuffer.length) {
    return false;
  }

  return timingSafeEqual(leftBuffer, rightBuffer);
};
