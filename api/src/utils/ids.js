import { createHash, randomBytes } from 'node:crypto';

export const createOpaqueId = ({ bytes = 18 } = {}) => randomBytes(bytes).toString('hex');

export const createCorrelationId = () => `cid-${createOpaqueId({ bytes: 12 })}`;

export const sha256Hex = ({ value }) => createHash('sha256').update(value).digest('hex');

export const createBootstrapPassword = () => randomBytes(18).toString('base64url');
