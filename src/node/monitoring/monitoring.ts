import type {
  CronStatus,
  IJob,
  IJobsCronTask,
  IJobsSimple,
  IWorker,
  JobStatus,
  ProcessorStatus,
  WorkerStatus,
} from "../../common/model.ts";
import {
  isJobCron,
  isJobCronTask,
  isJobSimple,
  isJobSimpleOrCronTask,
} from "../../common/model.ts";
import { getJobCollection, getWorkerCollection } from "../data/actions.ts";

function buildWorkerStatus(workers: IWorker[], jobs: IJob[]): WorkerStatus[] {
  return workers.map((worker): WorkerStatus => {
    const task =
      jobs.filter(isJobSimpleOrCronTask).find((job) => {
        return job.worker_id === worker._id;
      }) ?? null;

    return { worker, task };
  });
}

function buildQueueStatus(
  jobs: IJob[],
  now: Date,
): Array<IJobsSimple | IJobsCronTask> {
  const pending = jobs
    .filter(isJobSimpleOrCronTask)
    .filter((job) => job.status === "pending" && job.scheduled_for <= now);

  return pending;
}

function buildCronStatus(jobs: IJob[]): CronStatus[] {
  const crons = jobs.filter(isJobCron);

  return crons.map((cron) => {
    const tasks = jobs
      .filter(isJobCronTask)
      .filter((job) => job.name === cron.name);
    const scheduled = tasks.filter((job) => job.status === "pending");
    const running = tasks.filter(
      (job) => job.status === "running" || job.status === "paused",
    );
    const history = tasks.filter(
      (job) => job.status !== "pending" && job.status !== "running",
    );

    return {
      cron,
      scheduled,
      running,
      history,
    };
  });
}

function buildJobStatus(jobs: IJob[]): JobStatus[] {
  const names = Array.from(
    new Set(jobs.filter(isJobSimple).map((job) => job.name)),
  );

  return names.map((name) => {
    return {
      name,
      tasks: jobs.filter(isJobSimple).filter((job) => job.name === name),
    };
  });
}

export async function getProcessorStatus(): Promise<ProcessorStatus> {
  const now = new Date();
  const [workers, jobs] = await Promise.all([
    getWorkerCollection().find().toArray(),
    getJobCollection()
      .find({}, { sort: { scheduled_for: -1 } })
      .toArray(),
  ]);

  return {
    now,
    workers: buildWorkerStatus(workers, jobs),
    queue: buildQueueStatus(jobs, now),
    crons: buildCronStatus(jobs),
    jobs: buildJobStatus(jobs),
  };
}

export async function getProcessorHealthcheck() {
  const now = new Date();
  const [workers, jobs] = await Promise.all([
    getWorkerCollection().find().toArray(),
    getJobCollection()
      .find(
        { status: { $nin: ["finished", "errored"] } },
        { sort: { scheduled_for: -1 } },
      )
      .toArray(),
  ]);

  return {
    now,
    workers: buildWorkerStatus(workers, jobs),
    queue: buildQueueStatus(jobs, now),
  };
}
