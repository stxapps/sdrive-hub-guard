import { Logging } from '@google-cloud/logging';

import { isObject, isFldStr, randomString } from './utils.js';

const logging = new Logging();

const retrieveLogs = async (logKey) => {
  const oneHourAgo = (new Date(Date.now() - (3 * 60 * 60 * 1000))).toISOString();
  const options = {
    filter: `
      resource.type="gae_app"
      timestamp>="${oneHourAgo}"
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

const getLogsPerAddr = (logs) => {
  const logsPerKey = {};
  for (const log of logs) {
    const text = log.data;
    if (!isFldStr(text) || !text.startsWith('(')) continue;

    const i = text.indexOf(')');
    if (i <= 1) continue;

    const key = text.slice(1, i);
    if (!Array.isArray(logsPerKey[key])) logsPerKey[key] = [];
    logsPerKey[key].push(log);
  }

  const logsPerAddr = {};
  for (const kLogs of Object.values<any>(logsPerKey)) {
    let addr;
    for (const log of kLogs) {
      const text = log.data;
      const i = text.indexOf('address:');
      if (i >= 0) {
        addr = text.slice(i + 8).trim();
        break;
      }
    }
    if (!isFldStr(addr)) continue;

    if (!Array.isArray(logsPerAddr[addr])) logsPerAddr[addr] = [];
    logsPerAddr[addr].push(kLogs);
  }

  return logsPerAddr;
};

const printAddrCount = (logsPerAddr) => {
  const counts: any[] = [];

  for (const [addr, aLogs] of Object.entries<any>(logsPerAddr)) {
    counts.push({ addr, nLogs: aLogs.length });
  }

  counts.sort((a, b) => b.nLogs - a.nLogs);

  for (const { addr, nLogs } of counts) {
    console.log(`addr: ${addr}, nLogs: ${nLogs}`);
  }
};

const main = async () => {
  const startDate = new Date();
  const logKey = `${startDate.getTime()}-${randomString(4)}`;
  console.log(`(${logKey}) Worker starts on ${startDate.toISOString()}`);

  const logs = await retrieveLogs(logKey);
  const logsPerAddr = getLogsPerAddr(logs)
  printAddrCount(logsPerAddr);

  console.log(`(${logKey}) Worker finishes on ${(new Date()).toISOString()}.`);
};
main();
