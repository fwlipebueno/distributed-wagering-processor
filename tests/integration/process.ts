import { createApplication } from "../../src/http/application.js";
import { readConfig } from "../../src/infrastructure/config.js";
import type { WorkerHooks } from "../../src/infrastructure/workers.js";

const mode = process.argv[2];
const hooks: WorkerHooks = {};
if (mode === "crash-after-commit" || mode === "graceful-inflight") {
  hooks.afterCommit = async (messageId, transactionId) => {
    console.log(
      JSON.stringify({ marker: "afterCommit", messageId, transactionId }),
    );
    if (mode === "graceful-inflight") await Bun.sleep(300);
    else await new Promise<void>(() => {});
  };
}
if (mode === "crash-after-publish") {
  hooks.afterPublish = async (eventId) => {
    console.log(JSON.stringify({ marker: "afterPublish", eventId }));
    await new Promise<void>(() => {});
  };
}
const application = await createApplication({ config: readConfig(), hooks });
const stop = () => {
  void application.close().then(() => process.exit(0));
};
process.once("SIGTERM", stop);
process.once("SIGINT", stop);
console.log(JSON.stringify({ marker: "ready", pid: process.pid, mode }));
