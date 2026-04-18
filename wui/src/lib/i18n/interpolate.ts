export const interpolate = (template: string, values: Record<string, unknown> | unknown[], fallback: string | boolean = '') => {
  const isArr = Array.isArray(values);
  const pattern = isArr ? /{#([1-9][0-9]*|n)}/g : /{\$([a-zA-Z_][a-zA-Z0-9_]*)}/g;

  let index = 0;

  return template.replace(pattern, (match, key) => {
    let value: unknown;

    if (isArr) {
      if (key === 'n') {
        value = values[index];
        index += 1;
      } else {
        value = values[Number.parseInt(key, 10) - 1];
      }
    } else {
      value = values[key];
    }

    if (value !== undefined) {
      return String(value);
    }

    return fallback === true ? match : String(fallback);
  });
};
