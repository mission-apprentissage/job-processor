import { Db } from "mongodb";
import { IJobsSimple } from "./data/model.ts";
import { configureDbSchemaValidation } from "./data/actions.ts";

interface ILogger {
  debug(msg: string): unknown;
  info(data: Record<string, unknown>, msg: string): unknown;
  info(msg: string): unknown;
  child(data: Record<string, unknown>): ILogger;
  error(data: Record<string, unknown>, msg: string | Error): unknown;
}

export type JobFn = (job: IJobsSimple, signal: AbortSignal) => Promise<void>;

export type CronDef = {
  cron_string: string;
  handler: (signal: AbortSignal) => Promise<unknown>;
};

export type JobProcessorOptions = {
  db: Db;
  logger: ILogger;
  jobs: Record<string, JobFn>;
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
  await configureDbSchemaValidation();
}
