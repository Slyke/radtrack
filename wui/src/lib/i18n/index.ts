import { writable } from 'svelte/store';
import enUS from './en-US.json';
import { interpolate } from './interpolate';

type MessageDictionary = Record<string, string>;

const defaultLanguage = 'en-US';

const catalogs: Record<string, MessageDictionary> = {
  'en-US': enUS
};

const resolveCatalog = ({ language }: { language?: string | null }) => {
  const normalizedLanguage = typeof language === 'string' && language.trim()
    ? language.trim()
    : defaultLanguage;
  const messages = catalogs[normalizedLanguage] ?? catalogs[defaultLanguage];

  return {
    language: catalogs[normalizedLanguage] ? normalizedLanguage : defaultLanguage,
    messages
  };
};

const initialCatalog = resolveCatalog({ language: defaultLanguage });

export const localeStore = writable(initialCatalog);

export const setLocale = ({ language }: { language?: string | null }) => {
  localeStore.set(resolveCatalog({ language }));
};

export const translateMessage = ({
  key,
  values = {},
  messages
}: {
  key: string;
  values?: Record<string, unknown>;
  messages?: MessageDictionary;
}) => {
  const template = messages?.[key] ?? catalogs[defaultLanguage][key] ?? key;
  return interpolate(template, values);
};

export const formatDateTime = ({
  value,
  language
}: {
  value: string | null | undefined;
  language?: string | null;
}) => {
  if (!value) {
    return null;
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  void language;

  const pad = (input: number) => String(input).padStart(2, '0');

  return [
    `${parsed.getFullYear()}-${pad(parsed.getMonth() + 1)}-${pad(parsed.getDate())}`,
    `${pad(parsed.getHours())}:${pad(parsed.getMinutes())}:${pad(parsed.getSeconds())}`
  ].join(' ');
};
