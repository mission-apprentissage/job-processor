import { Jsonify } from "type-fest";
import { z } from "zod";
import { zObjectId } from "zod-mongodb-schema";

const zCronName = z.string().describe("Le nom de la tâche");

export const ZJobSimple = z.object({
  _id: zObjectId,
  name: z.string().describe("Le nom de la tâche"),
  type: z.literal("simple"),
  status: z
    .enum(["pending", "running", "finished", "errored", "paused"])
    .describe("Statut courant du job"),
  sync: z.boolean().describe("Si le job est synchrone"),
  payload: z
    .record(z.unknown())
    .nullish()
    .describe("La donnée liéé à la tâche"),
  output: z
    .object({
      duration: z.string(),
      result: z.unknown(),
      error: z.string().nullable(),
    })
    .nullish()
    .describe("Les valeurs de retours du job"),
  scheduled_for: z.date().describe("Date de lancement programmée"),
  started_at: z.date().nullish().describe("Date de lancement"),
  ended_at: z.date().nullish().describe("Date de fin d'execution"),
  updated_at: z.date().describe("Date de mise à jour en base de données"),
  created_at: z.date().describe("Date d'ajout en base de données"),
  worker_id: zObjectId
    .nullable()
    .describe("Worker ID handling the job when running"),
});

export const ZJobCron = z.object({
  _id: zObjectId,
  name: zCronName,
  type: z.literal("cron"),
  status: z.enum(["active"]).describe("Statut courant du cron"),
  cron_string: z
    .string()
    .describe("standard cron string exemple: '*/2 * * * *'"),
  scheduled_for: z.date().describe("Date de lancement programmée"),
  updated_at: z.date().describe("Date de mise à jour en base de données"),
  created_at: z.date().describe("Date d'ajout en base de données"),
});

export const ZJobCronTask = z.object({
  _id: zObjectId,
  name: zCronName,
  type: z.literal("cron_task"),
  status: z
    .enum(["pending", "running", "finished", "errored", "paused"])
    .describe("Statut courant du job"),
  scheduled_for: z.date().describe("Date de lancement programmée"),
  started_at: z.date().nullish().describe("Date de lancement"),
  ended_at: z.date().nullish().describe("Date de fin d'execution"),
  updated_at: z.date().describe("Date de mise à jour en base de données"),
  created_at: z.date().describe("Date d'ajout en base de données"),
  output: z
    .object({
      duration: z.string(),
      result: z.unknown(),
      error: z.string().nullable(),
    })

    .nullish()
    .describe("Les valeurs de retours du job"),
  worker_id: zObjectId
    .nullable()
    .describe("Worker ID handling the job when running"),
  sentry_id: z
    .string()
    .nullish()
    .describe("Id sentry pour le tracking d'exécution des jobs"),
});

export const ZJob = z.discriminatedUnion("type", [
  ZJobSimple,
  ZJobCron,
  ZJobCronTask,
]);

export const ZWorker = z.object({
  _id: zObjectId,
  hostname: z.string().describe("Hostname du worker"),
  lastSeen: z.date().describe("Date du dernier heartbeat reçu"),
  tags: z
    .string()
    .array()
    .nullable()
    .describe("Liste des tags du worker à executer"),
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
