import { createApplication } from "./http/application.js";
import { log } from "./infrastructure/observability.js";

try {
  const application = await createApplication();
  const shutdown = () => {
    void application
      .close()
      .then(() => {
        process.exitCode = 0;
      })
      .catch(() => {
        log("error", "shutdown_failed", {}, "INFRASTRUCTURE_ERROR");
        process.exitCode = 1;
      });
  };
  process.once("SIGTERM", shutdown);
  process.once("SIGINT", shutdown);
} catch {
  log("error", "startup_failed", {}, "DEPENDENCY_UNAVAILABLE");
  process.exitCode = 1;
}
