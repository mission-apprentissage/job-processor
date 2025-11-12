import { EventEmitter } from "node:events";
import { detectExitedJobs, pickNextJob } from "../data/actions.ts";
import { getLogger } from "../logger.ts";
import { sleep } from "../../utils/sleep.ts";
import { executeJob, reportJobCrash } from "./worker.ts";

export const processorEventEmitter = new EventEmitter();

export async function runJobProcessor(signal: AbortSignal): Promise<void> {
  while (!signal.aborted) {
    const exitedJob = await detectExitedJobs();
    if (exitedJob) {
      await reportJobCrash(exitedJob);
    }

    getLogger().debug(`Process jobs queue - looking for a job to execute`);
    const nextJob = await pickNextJob();

    if (nextJob) {
      getLogger().info({ job: nextJob.name }, "job will start");
      await executeJob(nextJob, signal);
    } else {
      await sleep(45_000, signal); // 45 secondes
    }

    processorEventEmitter.emit("continue");
  }

  getLogger().info("job-processor: abort requested - stopping processor");
}
