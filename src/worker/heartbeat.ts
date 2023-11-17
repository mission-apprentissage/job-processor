import { ObjectId } from "mongodb";
import { getWorkerCollection } from "../data/actions.ts";
import os from "node:os";
import { getOptions } from "../setup.ts";
import { captureException } from "@sentry/node";
import { EventEmitter } from "node:events";

export const heartbeatEvent = new EventEmitter();

export async function startHeartbeat(signal: AbortSignal): Promise<ObjectId> {
  const _id = new ObjectId();

  await getWorkerCollection().insertOne({
    _id,
    hostname: os.hostname(),
    lastSeen: new Date(),
  });

  const intervalId = setInterval(
    async () => {
      try {
        const result = await getWorkerCollection().updateOne(
          { _id },
          { $set: { lastSeen: new Date() } },
        );
        heartbeatEvent.emit("ping");

        // We were detected as died by others, just exit abrutly
        // This can be caused by process not releasing the event loop for more than 5min
        if (result.modifiedCount === 0) {
          const error = new Error(
            "job-processor: worker has been detected as died",
          );
          throw error;
        }
      } catch (error) {
        // Error when processor is aborted are expected
        if (signal.aborted) {
          return;
        }

        getOptions().logger.error(
          { error },
          "job-processor: worker has been detected as died",
        );
        captureException(error);

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

    await getWorkerCollection().deleteOne({ _id });
    heartbeatEvent.emit("stop");
  });

  return _id;
}
