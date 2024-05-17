import { captureException } from "@sentry/node";
import { cronsInit, startCronScheduler } from "./crons/crons.ts";
import { createJobSimple } from "./data/actions.ts";
import { IJobsSimple } from "../common/model.ts";
import { getLogger } from "./setup.ts";
import { startHeartbeat, startSyncHeartbeat } from "./worker/heartbeat.ts";
import { executeJob } from "./worker/worker.ts";
import { runJobProcessor } from "./worker/processor.ts";

type ScheduleJobParams = Pick<IJobsSimple, "name" | "payload"> &
  Partial<Pick<IJobsSimple, "scheduled_for">>;

type AddJobSimpleParams = ScheduleJobParams & { queued?: boolean };

export async function scheduleJob({
  name,
  payload,
  scheduled_for = new Date(),
}: ScheduleJobParams): Promise<IJobsSimple> {
  const job = await createJobSimple({
    name,
    payload,
    scheduled_for,
    sync: false,
  });

  return job;
}

export async function createAndRunJob({
  name,
  payload,
}: Pick<IJobsSimple, "name" | "payload">): Promise<number> {
  const job = await createJobSimple({
    name,
    payload,
    scheduled_for: new Date(),
    sync: true,
  });

  const finallyCb = await startSyncHeartbeat();

  return executeJob(job, null).finally(finallyCb);
}

export async function addJob({
  name,
  payload,
  scheduled_for = new Date(),
  queued = false,
}: AddJobSimpleParams): Promise<number> {
  if (queued) {
    return scheduleJob({ name, payload, scheduled_for }).then(() => 0);
  }

  return createAndRunJob({ name, payload });
}

export async function startJobProcessor(signal: AbortSignal): Promise<void> {
  const teardownHeartbeat = await startHeartbeat(true, signal);

  try {
    if (signal.aborted) return;

    await cronsInit();

    if (signal.aborted) return;

    await startCronScheduler(signal);

    if (signal.aborted) return;

    await runJobProcessor(signal);
  } catch (error) {
    if (signal.reason !== error) {
      getLogger().error({ error }, "Job processor crashed");
      captureException(error);
      throw error;
    }
  } finally {
    // heartbeat need to be awaited to let last mongodb updates to run
    await teardownHeartbeat();
    getLogger().info("Job processor stopped");
  }
}

export { initJobProcessor } from "./setup.ts";
export * from "../common/index.ts";
export type * from "../common/index.ts";

export { getProcessorStatus } from "./monitoring/monitoring.ts";

export { getSimpleJob } from "./data/actions.ts";
