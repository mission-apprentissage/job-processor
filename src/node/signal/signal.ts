import type { ChangeStreamInsertDocument } from "mongodb";
import { MongoError, ObjectId } from "mongodb";
import { getJobCollection, getSignalCollection } from "../data/actions.ts";
import type { ISignal } from "../../common/model.ts";
import { workerId } from "../worker/workerId.ts";
import { getLogger } from "../setup.ts";

export async function processSignal(signal: ISignal): Promise<void> {
  switch (signal.type) {
    case "kill":
      await killJob(signal.job_id);
      await getSignalCollection().updateOne(
        { _id: signal._id },
        { $set: { ack: true } },
      );
      break;
  }
}

async function isChangeStreamSupported(): Promise<boolean> {
  const changeStream = getSignalCollection().watch();
  try {
    await changeStream.tryNext();
    await changeStream.close();
    return true;
  } catch (e) {
    if (e instanceof MongoError && e.code === 40573) {
      return false;
    }
    throw e;
  }
}

export async function listenSignalCollection(
  signal: AbortSignal,
): Promise<void> {
  const isSupported = await isChangeStreamSupported();

  if (!isSupported) {
    const intervalId: NodeJS.Timeout = setInterval(
      async () => {
        const signals = await getSignalCollection()
          .find({ worker_id: workerId, ack: false })
          .toArray();

        for (const signal of signals) {
          await processSignal(signal);
        }
      },
      process.env["TEST"] ? 50 : 60_000,
    ).unref();

    signal.addEventListener("abort", () => {
      clearInterval(intervalId);
    });

    return;
  }

  const changeStream = getSignalCollection().watch([
    {
      $match: {
        operationType: "insert",
        "fullDocument.worker_id": workerId,
        "fullDocument.ack": false,
      },
    },
  ]);

  changeStream.on("error", async (error) => {
    getLogger().error({ error }, "signal-listener: change stream error");
    if (!signal.aborted) {
      await changeStream.close();
      await listenSignalCollection(signal);
    }
  });

  changeStream.on(
    "change",
    async (change: ChangeStreamInsertDocument<ISignal>) => {
      if (signal.aborted) {
        await changeStream.close();
        return;
      }

      await processSignal(change.fullDocument);
    },
  );

  signal.addEventListener(
    "abort",
    async () => {
      getLogger().info("signal-listener: abort requested - stopping listener");
      await changeStream.close();
    },
    { once: true },
  );

  // Process any signals that might have been missed between the last poll and the change stream opening
  const signals = await getSignalCollection()
    .find({ worker_id: workerId, ack: false })
    .toArray();

  for (const signal of signals) {
    await processSignal(signal);
  }
}

const context = new Map<string, AbortController>();

export function getJobKillSignal(jobId: ObjectId): AbortSignal {
  const controller = context.get(jobId.toHexString()) ?? new AbortController();
  context.set(jobId.toHexString(), controller);

  controller.signal.addEventListener(
    "abort",
    () => {
      context.delete(jobId.toHexString());
    },
    { once: true },
  );

  return controller.signal;
}

export function clearJobKillSignal(jobId: ObjectId): void {
  context.delete(jobId.toHexString());
}

export async function killJob(id: ObjectId): Promise<void> {
  getLogger().info({ jobId: id }, "job-processor: kill job requested");
  await getJobCollection().findOneAndUpdate(
    {
      _id: id,
      type: { $in: ["simple", "cron_task"] },
      status: { $in: ["pending", "paused"] },
    },
    { $set: { status: "killed" } },
    { returnDocument: "after", includeResultMetadata: false },
  );

  const job = await getJobCollection().findOne({ _id: id });

  if (!job || job.type === "cron") {
    throw new Error(`Job with id ${id.toHexString()} not found`);
  }

  if (job.status !== "running" || job.worker_id === null) {
    return;
  }

  if (!job.worker_id.equals(workerId)) {
    await getSignalCollection().insertOne({
      _id: new ObjectId(),
      type: "kill",
      job_id: id,
      worker_id: job.worker_id,
      created_at: new Date(),
      ack: false,
    });
    return;
  }

  const controller = context.get(id.toHexString());
  if (!controller) {
    // There is a timeframe where the job is running but the controller is not yet set
    // In that case, we just wait a bit and try again
    setTimeout(
      () => {
        killJob(id).catch((err) => {
          getLogger().error(
            { error: err, jobId: id },
            "job-processor: kill job retry failed",
          );
        });
      },
      process.env["TEST"] ? 50 : 1_000,
    ).unref();
    return;
  }

  controller.abort(new Error("job-processor: job killed"));
  return;
}
