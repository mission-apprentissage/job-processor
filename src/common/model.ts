import type { Jsonify } from "type-fest";
import { z } from "zod/v4-mini";
import { zObjectId } from "zod-mongodb-schema";

const zCronName = z.string();

export const ZJobSimple = z.object({
  _id: zObjectId,
  name: z.string(),
  type: z.literal("simple"),
  status: z.enum(["pending", "running", "finished", "errored", "paused"]),
  sync: z.boolean(),
  payload: z.nullish(z.record(z.string(), z.unknown())),
  output: z.nullish(
    z.object({
      duration: z.string(),
      result: z.unknown(),
      error: z.nullable(z.string()),
    }),
  ),
  scheduled_for: z.date(),
  started_at: z.nullish(z.date()),
  ended_at: z.nullish(z.date()),
  updated_at: z.date(),
  created_at: z.date(),
  worker_id: z.nullable(zObjectId),
});

export const ZJobCron = z.object({
  _id: zObjectId,
  name: zCronName,
  type: z.literal("cron"),
  status: z.enum(["active"]),
  cron_string: z.string(),
  scheduled_for: z.date(),
  updated_at: z.date(),
  created_at: z.date(),
});

export const ZJobCronTask = z.object({
  _id: zObjectId,
  name: zCronName,
  type: z.literal("cron_task"),
  status: z.enum(["pending", "running", "finished", "errored", "paused"]),
  scheduled_for: z.date(),
  started_at: z.nullish(z.date()),
  ended_at: z.nullish(z.date()),
  updated_at: z.date(),
  created_at: z.date(),
  output: z.nullish(
    z.object({
      duration: z.string(),
      result: z.unknown(),
      error: z.nullable(z.string()),
    }),
  ),
  worker_id: z.nullable(zObjectId),
  sentry_id: z.nullish(z.string()),
});

export const ZJob = z.discriminatedUnion("type", [
  ZJobSimple,
  ZJobCron,
  ZJobCronTask,
]);

export const ZWorker = z.object({
  _id: zObjectId,
  hostname: z.string(),
  lastSeen: z.date(),
  tags: z.nullable(z.array(z.string())),
});

export function isJobSimple(job: IJob): job is IJobsSimple {
  return job.type === "simple";
}

export function isJobCron(job: IJob): job is IJobsCron {
  return job.type === "cron";
}

export function isJobCronTask(job: IJob): job is IJobsCronTask {
  return job.type === "cron_task";
}

export function isJobSimpleOrCronTask(
  job: IJob,
): job is IJobsSimple | IJobsCronTask {
  return isJobSimple(job) || isJobCronTask(job);
}

export type CronName = z.output<typeof zCronName>;

export type IJob = z.output<typeof ZJob>;
export type IJobsSimple = z.output<typeof ZJobSimple>;
export type IJobsCron = z.output<typeof ZJobCron>;
export type IJobsCronTask = z.output<typeof ZJobCronTask>;

export type IWorker = z.output<typeof ZWorker>;

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
  tasks: z.array(ZJobSimple),
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
