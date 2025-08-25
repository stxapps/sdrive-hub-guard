import { Datastore } from '@google-cloud/datastore';
import { Storage } from '@google-cloud/storage';

import { isObject } from './utils.js';
import { BUCKET_INFO } from './const.js';

const datastore = new Datastore();
const storage = new Storage();

const bucket = storage.bucket('sdrive-001.appspot.com');

const listFPaths = async (logKey, prefix, maxResults) => {
  const [files] = await bucket.getFiles({ prefix, maxResults });
  const fpaths = files.map(f => f.name);
  return fpaths;
};

const listDirs = async (logKey, prefix) => {
  const res = await bucket.getFiles({ prefix, delimiter: '/', autoPaginate: false });
  const apiRes: any = res[2];
  const dirs = apiRes.prefixes.map(p => p.replace(prefix, '').replace('/', ''));
  return dirs;
};

export const getInfosPerAddr = async (logKey, addrs) => {
  const infosPerAddr = {};
  for (const addr of addrs) {
    let appName = '', nItems = 0, nLists = 0, nSettings = 0;
    let createDate: Date = new Date(0), updateDate: Date = new Date(0);

    const key = datastore.key([BUCKET_INFO, addr]);
    const [info] = await datastore.get(key);
    if (isObject(info)) {
      nItems = info.nItems;
      [createDate, updateDate] = [info.createDate, info.updateDate];
    } else {
      console.log(`(${logKey}) found invalid BucketInfo for addr:`, addr);
    }

    const lnkFPaths = await listFPaths(logKey, `${addr}/links/`, 1);
    const ntFPaths = await listFPaths(logKey, `${addr}/notes/`, 1);
    const stgFPaths = await listFPaths(logKey, `${addr}/settings`, 10);

    if (lnkFPaths.length > 0 && ntFPaths.length === 0) {
      appName = 'Brace.to';

      const listNames = await listDirs(logKey, `${addr}/links/`);
      nLists = listNames.length;
    } else if (ntFPaths.length > 0 && lnkFPaths.length === 0) {
      appName = 'Justnote';

      const listNames = await listDirs(logKey, `${addr}/notes/`);
      nLists = listNames.length;
    } else {
      console.log(`(${logKey}) found invalid lnkFPaths: ${lnkFPaths} and ntFPaths: ${ntFPaths} for addr:`, addr);
    }

    nSettings = stgFPaths.length;

    console.log(`addr: ${addr}, appName: ${appName}`);
    console.log(`  nItems: ${nItems}, nLists: ${nLists}, nSettings: ${nSettings}, ${createDate.toLocaleDateString()} - ${updateDate.toLocaleDateString()}`);
    infosPerAddr[addr] = { appName, nItems, nLists, nSettings, createDate, updateDate };
  }
  return infosPerAddr;
};
