import { Db } from "mongodb";
import { IJobsCronTask, IJobsSimple } from "./data/model.ts";
import { configureDb } from "./data/actions.ts";

export interface ILogger {
  debug(msg: string): unknown;
  info(data: Record<string, unknown>, msg: string): unknown;
  info(msg: string): unknown;
  child(data: Record<string, unknown>): ILogger;
  error(data: Record<string, unknown>, msg: string | Error): unknown;
}

export type JobDef = {
  handler: (job: IJobsSimple, signal: AbortSignal) => Promise<unknown>;
  // Particularly usefull to handle unexpected errors, crash & interruptions
  onJobExited?: (job: IJobsSimple) => Promise<unknown>;
};

export type CronDef = {
  cron_string: string;
  handler: (signal: AbortSignal) => Promise<unknown>;
  // Particularly usefull to handle unexpected errors, crash & interruptions
  onJobExited?: (job: IJobsCronTask) => Promise<unknown>;
};

export type JobProcessorOptions = {
  db: Db;
  logger: ILogger;
  jobs: Record<string, JobDef>;
  crons: Record<string, CronDef>;
};

let options: JobProcessorOptions | null = null;

export function getOptions(): JobProcessorOptions {
  if (!options) {
    throw new Error("Job processor is not setup");
  }

  return options;
}

export function getLogger(): ILogger {
  return getOptions().logger;
}

export async function initJobProcessor(opts: JobProcessorOptions) {
  options = opts;
  await configureDb();
}
