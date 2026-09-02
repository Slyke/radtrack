import test from 'node:test';
import assert from 'node:assert/strict';
import { createSettingsService } from '../src/services/settings.js';

const createUserSettingsHarness = () => {
  const rows = [];
  const db = {
    query: async (_sql, params) => ({
      rows: rows
        .filter((row) => row.user_id === params[0])
        .map((row) => ({
          key_name: row.key_name,
          value_json: row.value_json
        }))
    }),
    withTransaction: async (run) => run({
      query: async (_sql, params) => {
        const [userId, keyName, valueJson] = params;
        const existing = rows.find((row) => (
          row.user_id === userId
          && row.key_name === keyName
        ));
        if (existing) {
          existing.value_json = JSON.parse(valueJson);
        } else {
          rows.push({
            user_id: userId,
            key_name: keyName,
            value_json: JSON.parse(valueJson)
          });
        }
      }
    })
  };

  return {
    rows,
    service: createSettingsService({
      db,
      runtimeConfig: {
        aggregation: {
          cellCacheRefreshTtlOnRead: false
        }
      }
    })
  };
};

test('defaults information icons on for every user', async () => {
  const { service } = createUserSettingsHarness();

  const settings = await service.getUserSettings({ userId: 'user-1' });

  assert.equal(settings.showInfoIcons, true);
  assert.equal(settings.defaults.showInfoIcons, true);
});

test('persists information icon visibility as a user setting', async () => {
  const { rows, service } = createUserSettingsHarness();

  const settings = await service.updateUserSettings({
    userId: 'user-1',
    updates: {
      showInfoIcons: false
    }
  });

  assert.equal(settings.showInfoIcons, false);
  assert.deepEqual(rows.map((row) => ({
    userId: row.user_id,
    key: row.key_name,
    value: row.value_json
  })), [{
    userId: 'user-1',
    key: 'ui.showInfoIcons',
    value: false
  }]);
});

test('rejects a non-boolean information icon preference', async () => {
  const { service } = createUserSettingsHarness();

  await assert.rejects(
    service.updateUserSettings({
      userId: 'user-1',
      updates: {
        showInfoIcons: 'no'
      }
    }),
    (error) => (
      error?.status === 400
      && [
        'REQUEST_INVALID',
        'REQUEST_INVALID_SETTINGS_UPDATEUSERSETTINGS_SHOWINFOICONS_MUST_BE_A_BOOLEAN'
      ].includes(error?.errorKey)
    )
  );
});
