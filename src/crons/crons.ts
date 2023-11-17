import cronParser from "cron-parser";
import { CronDef, getLogger, getOptions } from "../setup.ts";
import {
  createJobCron,
  createJobCronTask,
  findJobCron,
  findJobs,
  getJobCollection,
  updateJob,
  updateJobCron,
} from "../data/actions.ts";
import { IJobsCron } from "../data/model.ts";
import { captureException } from "@sentry/node";

function parseCronString(
  cronString: string,
  options: { currentDate: string } | object = {},
) {
  return cronParser.parseExpression(cronString, {
    tz: "Europe/Paris",
    ...options,
  });
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
  getLogger().info(`Crons - initialise crons in DB`);

  let schedulerRequired = false;

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
    const cronJob = await findJobCron({
      name: cron.name,
    });

    if (!cronJob) {
      await createJobCron({
        name: cron.name,
        cron_string: cron.cron_string,
        scheduled_for: new Date(),
      });
      schedulerRequired = true;
    } else if (
      cronJob.type === "cron" &&
      cronJob.cron_string !== cron.cron_string
    ) {
      await updateJobCron(cronJob._id, cron.cron_string);
      await getJobCollection().deleteMany({
        name: cronJob.name,
        status: "pending",
        type: "cron_task",
      });
      schedulerRequired = true;
    }
  }

  if (schedulerRequired) {
    await cronsScheduler();
  }
}

export async function cronsScheduler(): Promise<void> {
  getLogger().info(`Crons - Check and run crons`);

  const crons = await findJobs<IJobsCron>(
    {
      type: "cron",
      scheduled_for: { $lte: new Date() },
    },
    { sort: { scheduled_for: 1 } },
  );

  for (const cron of crons) {
    const next = parseCronString(cron.cron_string ?? "", {
      currentDate: cron.scheduled_for,
    }).next();
    await createJobCronTask({
      name: cron.name,
      scheduled_for: next.toDate(),
    });

    await updateJob(cron._id, {
      scheduled_for: next.toDate(),
    });
  }
  const cron = await findJobCron({}, { sort: { scheduled_for: 1 } });

  if (!cron) return;

  const execCronScheduler = async () => {
    try {
      await cronsScheduler();
    } catch (err) {
      captureException(err);
      setTimeout(execCronScheduler, 1_000);
    }
  };

  // No need to block
  setTimeout(execCronScheduler, cron.scheduled_for.getSeconds() + 1).unref();
}
