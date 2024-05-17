import type { Jsonify } from "type-fest";
import { getJobCollection, getWorkerCollection } from "../data/actions.ts";
import {
  IJob,
  IJobsCronTask,
  IJobsSimple,
  IWorker,
  ZJobCron,
  ZJobCronTask,
  ZJobSimple,
  ZWorker,
  isJobCron,
  isJobCronTask,
  isJobSimple,
  isJobSimpleOrCronTask,
} from "../data/model.ts";
import { z } from "zod";

const zWorkerStatus = z.object({
  worker: ZWorker,
  task: z.union([ZJobSimple, ZJobCronTask, z.null()]),
});

const zCronStatus = z.object({
  cron: ZJobCron,
  scheduled: z.array(ZJobCronTask),
  running: z.array(ZJobCronTask),
  history: z.array(ZJobCronTask),
});

const zJobStatus = z.object({
  name: z.string(),
  tasks: z.array(z.union([ZJobSimple, ZJobCronTask])),
});

export const zProcessorStatus = z.object({
  now: z.date(),
  workers: z.array(zWorkerStatus),
  queue: z.array(z.union([ZJobSimple, ZJobCronTask])),
  jobs: z.array(zJobStatus),
  crons: z.array(zCronStatus),
});

export type WorkerStatus = z.output<typeof zWorkerStatus>;

export type CronStatus = z.output<typeof zCronStatus>;

export type JobStatus = z.output<typeof zJobStatus>;

export type ProcessorStatus = z.output<typeof zProcessorStatus>;

export type ProcessorStatusJson = Jsonify<ProcessorStatus>;

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
