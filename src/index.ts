import { cronsInit } from "./crons/crons.ts";
import { createJobSimple, pickNextJob } from "./data/actions.ts";
import { IJobsSimple } from "./data/model.ts";
import { getLogger } from "./setup.ts";
import { sleep } from "./utils/sleep.ts";
import { executeJob } from "./worker/worker.ts";

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
    return executeJob(job, null);
  }

  return 0;
}

async function runJobProcessor(signal: AbortSignal): Promise<void> {
  if (signal.aborted) {
    return;
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
  await cronsInit();
  await runJobProcessor(signal);
}

export { initJobProcessor } from "./setup.ts";