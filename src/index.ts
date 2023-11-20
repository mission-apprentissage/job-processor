import { cronsInit, startCronScheduler } from "./crons/crons.ts";
import {
  createJobSimple,
  detectExitedJobs,
  pickNextJob,
} from "./data/actions.ts";
import { IJobsSimple } from "./data/model.ts";
import { getLogger } from "./setup.ts";
import { sleep } from "./utils/sleep.ts";
import { startHeartbeat, startSyncHeartbeat } from "./worker/heartbeat.ts";
import { executeJob, reportJobCrash } from "./worker/worker.ts";

type AddJobSimpleParams = Pick<IJobsSimple, "name" | "payload"> &
  Partial<Pick<IJobsSimple, "scheduled_for">> & { queued?: boolean };

export async function addJob({
  name,
  payload,
  scheduled_for = new Date(),
  queued = false,
}: AddJobSimpleParams): Promise<number> {
  const job = await createJobSimple({
    name,
    payload,
    scheduled_for,
    sync: !queued,
  });

  if (!queued && job) {
    const finallyCb = await startSyncHeartbeat();

    return executeJob(job, null).finally(finallyCb);
  }

  return 0;
}

async function runJobProcessor(signal: AbortSignal): Promise<void> {
  if (signal.aborted) {
    return;
  }

  const exitedJob = await detectExitedJobs();
  if (exitedJob) {
    await reportJobCrash(exitedJob);
    return startJobProcessor(signal);
  }

  getLogger().debug(`Process jobs queue - looking for a job to execute`);
  const nextJob = await pickNextJob();

  if (nextJob) {
    getLogger().info({ job: nextJob.name }, "job will start");
    await executeJob(nextJob, signal);
  } else {
    await sleep(45_000, signal); // 45 secondes
  }

  return startJobProcessor(signal);
}

export async function startJobProcessor(signal: AbortSignal): Promise<void> {
  const teardownHeartbeat = await startHeartbeat(true, signal);

  if (signal.aborted) return;

  await cronsInit();

  if (signal.aborted) return;

  await startCronScheduler(signal);

  if (signal.aborted) return;

  await runJobProcessor(signal);

  // heartbeat need to be awaited to let last mongodb updates to run
  await teardownHeartbeat();
}

export { initJobProcessor } from "./setup.ts";
export * from "./data/model.ts";
export type * from "./data/model.ts";

export { getSimpleJob } from "./data/actions.ts";
