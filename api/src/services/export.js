import { createAppError } from '../lib/errors.js';

export const createExportService = ({ queryService, settingsService }) => {
  const exportJson = async ({ user, input, correlationId = null }) => {
    const exportRaw = Boolean(input?.includeRaw);
    const exportAggregates = Boolean(input?.includeAggregates);
    if (!exportRaw && !exportAggregates) {
      throw createAppError({
        caller: 'export::exportJson',
        reason: 'Export must include raw points, aggregates, or both.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const uiConfig = await settingsService.getUiConfig();
    const envelope = {
      title: 'Radiacode Track Export',
      type: 'radiacode-export',
      exportTime: new Date().toISOString(),
      metric: input?.metric ?? uiConfig.defaultMetric,
      filters: {
        datasetIds: input?.datasetIds ?? [],
        combinedDatasetIds: input?.combinedDatasetIds ?? [],
        dateFrom: input?.dateFrom ?? null,
        dateTo: input?.dateTo ?? null,
        applyExcludeAreas: Boolean(input?.applyExcludeAreas)
      },
      raw: null,
      aggregates: null
    };

    if (exportRaw) {
      envelope.raw = await queryService.getRawPoints({
        user,
        input: {
          ...input,
          limit: input?.limit ?? uiConfig.rawPointCap
        },
        correlationId
      });
    }

    if (exportAggregates) {
      envelope.aggregates = await queryService.getAggregates({
        user,
        input,
        correlationId
      });
    }

    return envelope;
  };

  return {
    exportJson
  };
};
