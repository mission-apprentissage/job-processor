import { ObjectId } from "mongodb";
import { getWorkerCollection } from "../data/actions.ts";
import os from "node:os";
import { getOptions } from "../setup.ts";
import { captureException } from "@sentry/node";
import { EventEmitter } from "node:events";

export const workerId = new ObjectId();

export const heartbeatEvent = new EventEmitter();

async function createWorker() {
  return getWorkerCollection().updateOne(
    { _id: workerId },
    {
      $set: { lastSeen: new Date() },
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

  if (syncHeartbeatContext.count === 1) {
    syncHeartbeatContext.ctrl = new AbortController();

    await startHeartbeat(false, syncHeartbeatContext.ctrl.signal);
  }

  return () => {
    syncHeartbeatContext.count--;
    if (syncHeartbeatContext.count === 0) {
      const ctrl = syncHeartbeatContext.ctrl;
      syncHeartbeatContext.ctrl = null;
      ctrl?.abort();
    }
  };
}

export async function startHeartbeat(
  exitOnError: boolean,
  signal: AbortSignal,
): Promise<void> {
  await createWorker();

  const intervalId = setInterval(
    async () => {
      try {
        const result = await getWorkerCollection().updateOne(
          { _id: workerId },
          { $set: { lastSeen: new Date() } },
        );

        // We were detected as died by others, just exit abrutly
        // This can be caused by process not releasing the event loop for more than 5min
        if (result.modifiedCount === 0) {
          const error = new Error(
            "job-processor: worker has been detected as died",
          );
          throw error;
        }

        heartbeatEvent.emit("ping");
      } catch (error) {
        // Error when processor is aborted are expected
        if (signal.aborted) {
          return;
        }

        getOptions().logger.error({ error }, "job-processor: heartbeat failed");
        captureException(error);

        if (!exitOnError) {
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
        // eslint-disable-next-line n/no-process-exit
        if (process.env["NODE_ENV"] !== "test") process.exit(1);
      }
    },
    // 30 seconds: needs to be way lower than expireAfterSeconds index
    30_000,
  ).unref();

  signal.addEventListener("abort", async () => {
    clearInterval(intervalId);

    await getWorkerCollection().deleteOne({ _id: workerId });
    heartbeatEvent.emit("stop");
  });
}
