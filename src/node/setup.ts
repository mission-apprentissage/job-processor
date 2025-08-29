import type { Db } from "mongodb";
import type { IJobsCronTask, IJobsSimple } from "../common/model.ts";
import { configureDb } from "./data/actions.ts";
import { getOptions, setOptions } from "./options.ts";

export interface ILogger {
  debug(msg: string): unknown;
  info(data: Record<string, unknown>, msg: string): unknown;
  info(msg: string): unknown;
  child(data: Record<string, unknown>): ILogger;
  error(data: Record<string, unknown>, msg: string): unknown;
}

export type JobDef<T extends string = string> = {
  handler: (job: IJobsSimple, signal: AbortSignal) => Promise<unknown>;
  // Particularly usefull to handle unexpected errors, crash & interruptions
  onJobExited?: (job: IJobsSimple) => Promise<unknown>;
  resumable?: boolean;
  tag?: T | null;
};

export type CronDef<T extends string = string> = {
  cron_string: string;
  handler: (signal: AbortSignal) => Promise<unknown>;
  // Particularly usefull to handle unexpected errors, crash & interruptions
  onJobExited?: (job: IJobsCronTask) => Promise<unknown>;
  resumable?: boolean;
  maxRuntimeInMinutes?: number;
  checkinMargin?: number;
  tag?: T | null;
};

export type JobProcessorOptions<T extends string = string> = {
  db: Db;
  logger: ILogger;
  jobs: Record<string, JobDef>;
  crons: Record<string, CronDef>;
  workerTags?: T[] | null;
};

export function getLogger(): ILogger {
  return getOptions().logger;
}

export async function initJobProcessor(opts: JobProcessorOptions) {
  if (opts.workerTags != null && opts.workerTags.length === 0) {
    throw new Error("workerTags should not be empty");
  }

  setOptions(opts);
  await configureDb();
}
