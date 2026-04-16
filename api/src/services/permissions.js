import { createAppError } from '../lib/errors.js';

export const roles = ['view_only', 'standard', 'moderator', 'admin'];

export const isRole = ({ role }) => roles.includes(role);

export const canImport = ({ user }) => ['standard', 'moderator', 'admin'].includes(user.role);

export const canShare = ({ user }) => ['standard', 'moderator', 'admin'].includes(user.role);

export const canModerateUsers = ({ user }) => ['moderator', 'admin'].includes(user.role);

export const canManageSystem = ({ user }) => user.role === 'admin';

export const ensureRole = ({ allowed, user, correlationId, reason = 'User is not allowed to perform this action.' }) => {
  if (allowed.includes(user.role)) {
    return;
  }

  throw createAppError({
    caller: 'permissions::ensureRole',
    reason,
    errorKey: 'AUTH_FORBIDDEN',
    correlationId,
    status: 403
  });
};
