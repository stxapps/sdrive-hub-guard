import { getInfosPerAddr } from './data.js';
import { randomString } from './utils.js';

const main = async () => {
  const startDate = new Date();
  const logKey = `${startDate.getTime()}-${randomString(4)}`;
  console.log(`(${logKey}) Worker starts on ${startDate.toISOString()}`);

  await getInfosPerAddr(logKey, ['']);

  console.log(`(${logKey}) Worker finishes on ${(new Date()).toISOString()}.`);
};
main();
