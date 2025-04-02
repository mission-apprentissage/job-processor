import { captureCheckIn } from "@sentry/node";
import { IJob, isJobCronTask } from "../../common/model.ts";
import { getJobCollection } from "../data/actions.ts";
import { getLogger, getOptions } from "../setup.ts";

export const notifySentryJobStart = async (job: IJob) => {
  if (!isJobCronTask(job)) {
    return;
  }
  const monitorConfig = getSentryMonitorConfig(job.name);
  if (!monitorConfig) {
    getLogger().error(
      { _id: job._id },
      `unexpected: could not find cron definition`,
    );
    return;
  }
  const checkInId = captureCheckIn(
    {
      monitorSlug: job.name,
      status: "in_progress",
    },
    monitorConfig,
  );
  await getJobCollection().updateOne(
    { _id: job._id },
    { $set: { sentry_id: checkInId } },
  );
};

export const notifySentryJobEnd = async (job: IJob, isSuccess: boolean) => {
  if (!isJobCronTask(job)) {
    return;
  }
  const monitorConfig = getSentryMonitorConfig(job.name);
  if (!monitorConfig) {
    getLogger().error(
      { _id: job._id },
      `unexpected: could not find cron definition`,
    );
    return;
  }
  const dbJob = await getJobCollection().findOne({ _id: job._id });
  if (!dbJob) {
    getLogger().error({ _id: job._id }, `unexpected: could not find job`);
    return;
  }
  if (!isJobCronTask(dbJob)) {
    getLogger().error({ _id: job._id }, `unexpected: not a cron task`);
    return;
  }
  const { sentry_id } = dbJob;
  if (!sentry_id) {
    getLogger().error({ _id: job._id }, `unexpected: no sentry_id`);
    return;
  }
  captureCheckIn(
    {
      checkInId: sentry_id,
      monitorSlug: job.name,
      status: isSuccess ? "ok" : "error",
    },
    monitorConfig,
  );
};

const getSentryMonitorConfig = (jobName: string) => {
  const cronDefOpt = getOptions().crons[jobName];
  if (!cronDefOpt) {
    return null;
  }
  return {
    schedule: {
      type: "crontab",
      value: cronDefOpt.cron_string,
    },
    checkinMargin: cronDefOpt.checkinMargin ?? 5, // In minutes. Optional.
    maxRuntime: cronDefOpt.maxRuntimeInMinutes ?? 60, // In minutes. Optional.
    timezone: "Europe/Paris", // Optional.
  } as const;
};
