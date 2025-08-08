import path from 'path';
import url from 'url';
import { Logging } from '@google-cloud/logging';
import { Datastore } from '@google-cloud/datastore';
import { FirewallClient } from '@google-cloud/appengine-admin';
import protobuf from 'protobufjs';

import { isObject, isFldStr, randomString } from './utils.js';
import { BLACKLIST } from './const.js';

const DURATION = 3 * 60 * 60 * 1000;
const MAX_N_REQS = 360;

const logging = new Logging();
const datastore = new Datastore();
const firewall = new FirewallClient();

const ptbRt = new protobuf.Root();
ptbRt.resolvePath = (_, target) => {
  const __filename = url.fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const rootDir = path.resolve(__dirname, "protos");
  return path.join(rootDir, target);
};
ptbRt.loadSync('google/appengine/logging/v1/request_log.proto');
const ReqLogProtobuf = ptbRt.lookupType('google.appengine.logging.v1.RequestLog');

const getLogs = async (logKey) => {
  const ts = (new Date(Date.now() - DURATION)).toISOString();
  const options = {
    filter: `
      resource.type="gae_app"
      timestamp>="${ts}"
    `,
    pageSize: 1000,
    orderBy: 'timestamp desc',
  };

  const logs: any[] = [];

  let opts: any = options;
  for (let i = 0; i < 99; i++) {
    const [entries, nextQuery] = await logging.getEntries(opts);
    console.log(`(${logKey}) got entries`);
    logs.push(...entries);

    if (!isObject(nextQuery) || !isFldStr(nextQuery.pageToken)) break;
    opts = nextQuery;
  }
  console.log(`(${logKey}) got ${logs.length} entries`);

  return logs;
};

const getDataFrame = (logKey, logs) => {
  const df: any[] = []; // [{ type, key, addr, trace, log }, ...]
  for (const log of logs) {
    let type: string, key = null, addr = null, trace = null;
    if (isFldStr(log.data) && log.data.startsWith('(')) {
      type = 'App';

      let i = log.data.indexOf(')');
      if (i > 1) {
        key = log.data.slice(1, i);
      }

      i = log.data.indexOf('address:');
      if (i >= 0) {
        addr = log.data.slice(i + 8).trim();
      }

      i = log.data.indexOf('trace:');
      if (i >= 0) {
        trace = log.data.slice(i + 6).trim().split('-')[1];
      }
    } else if (isObject(log.metadata) && isFldStr(log.metadata.trace)) {
      type = 'Req';
      trace = log.metadata.trace.split('/').slice(-1)[0];
    } else {
      console.log(`(${logKey}) found unsupported log:`, log);
      continue;
    }
    df.push({ type, key, addr, trace, log });
  }
  return df;
};

const putDataPerAddr = (dataPerAddr, addr, key, appLog, reqLog) => {
  if (!isFldStr(addr) || !isFldStr(key)) return;

  if (!isObject(dataPerAddr[addr])) dataPerAddr[addr] = {}
  if (!isObject(dataPerAddr[addr][key])) {
    dataPerAddr[addr][key] = { reqLogs: [], appLogs: [] };
  }
  if (isObject(appLog)) dataPerAddr[addr][key].appLogs.push(appLog);
  if (isObject(reqLog)) dataPerAddr[addr][key].reqLogs.push(reqLog);
};

const getDataPerAddr = (logKey, df) => {
  /*
    dataPerAddr = {
      addr: {
        key: { reqLogs: [...], appLogs: [...] },
        ...
      },
      ...
    };
  */

  const keyToAddr = {}, traceToKey = {};
  for (const { type, key, addr, trace } of df) {
    if (type !== 'App' || !isFldStr(key)) continue;
    if (isFldStr(addr)) keyToAddr[key] = addr;
    if (isFldStr(trace)) traceToKey[trace] = key;
  }

  const dataPerAddr = {};
  for (const { type, log, ...attrs } of df) {
    if (type === 'App' && isFldStr(attrs.key)) {
      const key = attrs.key;
      const addr = keyToAddr[key];
      putDataPerAddr(dataPerAddr, addr, key, log, null);
    } else if (type === 'Req' && isFldStr(attrs.trace)) {
      const key = traceToKey[attrs.trace];
      const addr = keyToAddr[key];
      putDataPerAddr(dataPerAddr, addr, key, null, log);
    } else {
      console.log(`(${logKey}) found invalid df item:`, log);
    }
  }

  return dataPerAddr;
};

const getIp = (logKey, log) => {
  try {
    const message = ReqLogProtobuf.decode(log.metadata.protoPayload.value);
    const payload = ReqLogProtobuf.toObject(message, {
      longs: String, // Ensure 64-bit integers are represented as strings
      enums: String, // Represent enums as strings
      bytes: String // Represent bytes as strings
    });
    if (isFldStr(payload.ip)) return payload.ip;
  } catch (error) {
    console.log(`(${logKey}) Failed to get ip:`, log, error);
  }
  return '';
};

const getCounts = (logKey, dataPerAddr) => {
  const counts: any[] = [];
  for (const addr in dataPerAddr) {
    let nReqs = 0, ips: string[] = [];
    for (const key in dataPerAddr[addr]) {
      nReqs += 1;
      for (const log of dataPerAddr[addr][key].reqLogs) {
        const ip = getIp(logKey, log);
        if (!ips.includes(ip)) ips.push(ip);
      }
    }

    counts.push({ addr, nReqs, ips });
  }
  counts.sort((a, b) => b.nReqs - a.nReqs);

  for (const { addr, nReqs } of counts) {
    console.log(`addr: ${addr}, nReqs: ${nReqs}`);
  }

  return counts;
};

const blacklist = async (logKey, counts) => {
  // if within the duration, nReqs is more than MAX_N_LOGS,
  //  add the address to the blacklist and ips to the firewall rules.
  for (const { addr, nReqs, ips } of counts) {
    if (nReqs <= MAX_N_REQS) continue;

    const key = datastore.key([BLACKLIST, addr]);
    const data = [
      { name: 'type', value: 1 },
      { name: 'createDate', value: new Date() },
    ];
    await datastore.save({ key, data: data });
    console.log(`(${logKey}) blacklist addr: ${addr}`);

    for (const ip of ips) {
      const rule = { action: 2, sourceRange: ip };
      await firewall.createIngressRule({ rule });
      console.log(`(${logKey}) deny ip: ${ip}`);
    }
  }
};

const main = async () => {
  const startDate = new Date();
  const logKey = `${startDate.getTime()}-${randomString(4)}`;
  console.log(`(${logKey}) Worker starts on ${startDate.toISOString()}`);

  const logs = await getLogs(logKey);
  const df = getDataFrame(logKey, logs)
  const dataPerAddr = getDataPerAddr(logKey, df);
  const counts = getCounts(logKey, dataPerAddr);
  await blacklist(logKey, counts)

  console.log(`(${logKey}) Worker finishes on ${(new Date()).toISOString()}.`);
};
main();
