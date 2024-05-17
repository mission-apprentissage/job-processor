import cronParser from "cron-parser";
import { CronDef, getLogger, getOptions } from "../setup.ts";
import {
  createJobCronTask,
  findJobs,
  getJobCollection,
} from "../data/actions.ts";
import { IJobsCron } from "../../common/model.ts";
import { ObjectId } from "mongodb";
import { captureException } from "@sentry/node";
import { EventEmitter } from "node:events";

function parseCronString(
  now: Date,
  cronString: string,
  options: { currentDate: string } | object = {},
): Date {
  const iterator = cronParser.parseExpression(cronString, {
    tz: "Europe/Paris",
    ...options,
  });

  const next = iterator.next().toDate();

  if (next.getTime() >= now.getTime()) {
    return next;
  }

  return now;
}

interface Cron extends CronDef {
  name: string;
}

function getCrons(): Cron[] {
  return Object.entries(getOptions().crons).map(([name, cronDef]) => ({
    ...cronDef,
    name,
  }));
}

export async function cronsInit() {
  getLogger().debug(`Crons - initialise crons in DB`);

  const CRONS = getCrons();

  await getJobCollection().deleteMany({
    name: { $nin: CRONS.map((c) => c.name) },
    type: "cron",
  });
  await getJobCollection().deleteMany({
    name: { $nin: CRONS.map((c) => c.name) },
    status: "pending",
    type: "cron_task",
  });

  for (const cron of CRONS) {
    // Atomic operation to prevent concurrency conflict

    const now = new Date();
    const result = await getJobCollection().findOneAndUpdate(
      {
        name: cron.name,
        type: "cron",
      },
      {
        $set: {
          cron_string: cron.cron_string,
          updated_at: now,
        },
        $setOnInsert: {
          _id: new ObjectId(),
          name: cron.name,
          type: "cron",
          status: "active",
          created_at: now,
          scheduled_for: now,
        },
      },
      {
        returnDocument: "before",
        upsert: true,
      },
    );

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const oldJob: IJobsCron | null = result as any;

    if (oldJob === null) {
      // upsert is not atomic, make sure we didn't created a duplicate
      const existingCrons = await getJobCollection()
        .find(
          {
            name: cron.name,
            type: "cron",
          },
          {
            sort: { _id: 1 },
          },
        )
        .toArray();

      if (existingCrons.length > 1 && existingCrons[0]) {
        // Just keep the first one
        await getJobCollection().deleteMany({
          name: cron.name,
          type: "cron",
          _id: { $ne: existingCrons[0]._id },
        });
      }
    }

    if (oldJob !== null && oldJob.cron_string !== cron.cron_string) {
      await getJobCollection().updateOne(
        { _id: oldJob._id },
        { $set: { scheduled_for: now, updated_at: now } },
      );
      await getJobCollection().deleteMany({
        name: cron.name,
        status: "pending",
        type: "cron_task",
      });
    }
  }
}

export const cronSchedulerEvent = new EventEmitter();

export async function runCronsScheduler(): Promise<void> {
  getLogger().debug(`Crons - Check and run crons`);

  const now = new Date();
  const crons = await findJobs<IJobsCron>(
    {
      type: "cron",
      scheduled_for: { $lte: now },
    },
    { sort: { scheduled_for: 1 } },
  );

  for (const cron of crons) {
    const next = parseCronString(now, cron.cron_string ?? "", {
      currentDate: cron.scheduled_for,
    });

    // Ensure no concurrent worker scheduled this cron already
    const result = await getJobCollection().updateOne(
      { _id: cron._id, scheduled_for: cron.scheduled_for },
      { $set: { scheduled_for: next, updated_at: new Date() } },
    );

    // Otherwise is already scheduled by another worker
    if (result.modifiedCount > 0) {
      await createJobCronTask({
        name: cron.name,
        scheduled_for: next,
      });
    }
  }

  cronSchedulerEvent.emit("updated");
}

export async function startCronScheduler(signal: AbortSignal) {
  const CRONS = getCrons();
  if (CRONS.length === 0) return;

  await runCronsScheduler();
  const intervalID = setInterval(async () => {
    try {
      await runCronsScheduler();
    } catch (err) {
      if (!signal.aborted) {
        captureException(err);
      }
    }
  }, 60_000).unref();

  signal.addEventListener(
    "abort",
    () => {
      clearInterval(intervalID);
    },
    { once: true },
  );
}
