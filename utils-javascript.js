export default function ensureArray(value) {
  if (!value) {
    return [];
  } else if (!Array.isArray(value)) {
    return [value];
  } else {
    return value;
  }
}

export function collapsArrayOfObjects(array, keysToKeep, keyToCollapse, collapsedKeyName) {
  const results = [];
  const keys = ensureArray(keysToKeep);
  array.forEach((a) => {
    let found = results.find(r => keys.every(k => a[k] === r[k]));
    if (found) {
      found[collapsedKeyName].push(a[keyToCollapse]);
    } else {
      const obj = {};
      for(const k of keys) {
        obj[k] = a[k];
      }
      obj[collapsedKeyName] = [a[keyToCollapse]];
      results.push(obj);
    }
  });
  return results;
}