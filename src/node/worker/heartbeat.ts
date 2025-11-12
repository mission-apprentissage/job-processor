import os from "node:os";
import { EventEmitter } from "node:events";
import { captureException, flush } from "@sentry/node";
import { getWorkerCollection } from "../data/actions.ts";
import { getLogger } from "../logger.ts";
import { getOptions } from "../options.ts";
import { workerId } from "./workerId.ts";

export const heartbeatEvent = new EventEmitter();

async function createWorker() {
  return getWorkerCollection().updateOne(
    { _id: workerId },
    {
      $set: { lastSeen: new Date(), tags: getOptions().workerTags ?? null },
      $setOnInsert: {
        hostname: os.hostname(),
      },
    },
    { upsert: true },
  );
}

const syncHeartbeatContext: { count: number; ctrl: AbortController | null } = {
  count: 0,
  ctrl: null,
};

// Sync heartbeat is used in the context of addJob sync
// We still need to add workerId to be able to detect crashes
// So the process will report heartbeat as long as it is running a job
// But we don't want to multiply heartbeat requests with concurrent addJob
export async function startSyncHeartbeat(): Promise<() => void> {
  syncHeartbeatContext.count++;

  let teardown: null | (() => Promise<null>) = null;
  if (syncHeartbeatContext.count === 1) {
    syncHeartbeatContext.ctrl = new AbortController();

    teardown = await startHeartbeat(false, syncHeartbeatContext.ctrl.signal);
  }

  return async () => {
    syncHeartbeatContext.count--;
    if (syncHeartbeatContext.count === 0) {
      const ctrl = syncHeartbeatContext.ctrl;
      syncHeartbeatContext.ctrl = null;
      ctrl?.abort();
      await teardown?.();
    }
  };
}

export async function startHeartbeat(
  isWorker: boolean,
  signal: AbortSignal,
): Promise<() => Promise<null>> {
  await createWorker();

  let successiveErrorsCount = 0;

  const intervalId = setInterval(
    async () => {
      try {
        const result = await getWorkerCollection().updateOne(
          { _id: workerId },
          { $set: { lastSeen: new Date() } },
        );

        // We were detected as died by others, just exit abrutly
        // This can be caused by process not releasing the event loop for more than 5min
        if (result.matchedCount === 0) {
          const error = new Error(
            "job-processor: worker has been detected as died",
          );
          throw error;
        }

        heartbeatEvent.emit("ping");
        successiveErrorsCount = 0;
      } catch (error) {
        // Error when processor is aborted are expected
        if (signal.aborted) {
          return;
        }

        successiveErrorsCount++;

        getOptions().logger.error({ error }, "job-processor: heartbeat failed");
        captureException(error, { extra: { workerId, successiveErrorsCount } });

        if (successiveErrorsCount < 3) {
          heartbeatEvent.emit("fail");
          getOptions().logger.info("job-processor: waiting for next check");
          return;
        }

        if (!isWorker) {
          // Force recreation in case it was removed
          // And keep trying
          return createWorker()
            .then(() => {
              heartbeatEvent.emit("ping");
            })
            .catch(() => {
              // Silent
            });
        }

        heartbeatEvent.emit("kill");
        clearInterval(intervalId);

        // Any failure in heartbeat is critical, just kill the process to prevent race conditions
        // Do not exit on tests
        if (process.env["NODE_ENV"] !== "test") {
          await flush();
          // eslint-disable-next-line n/no-process-exit
          process.exit(1);
        }
      }
    },
    // 30 seconds: needs to be way lower than expireAfterSeconds index
    30_000,
  ).unref();

  const teardownPromise: Promise<null> = new Promise((resolve) => {
    signal.addEventListener(
      "abort",
      async () => {
        clearInterval(intervalId);

        getLogger().info("job-processor: abort requested - stopping heartbeat");

        if (isWorker) {
          await getWorkerCollection()
            .deleteOne({ _id: workerId })
            .catch((error) => {
              getLogger().error(
                { error },
                "job-processor: worker self-removal failed",
              );
              captureException(error, { extra: { workerId } });
            });
        }

        getLogger().info("job-processor: abort requested - heartbeat stopped");
        heartbeatEvent.emit("stop");
        resolve(null);
      },
      { once: true },
    );
  });

  return async () => teardownPromise;
}
