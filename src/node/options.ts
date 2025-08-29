import type { JobProcessorOptions } from "./setup.ts";

let options: JobProcessorOptions | null = null;

export function getOptions(): JobProcessorOptions {
  if (!options) {
    throw new Error("Job processor is not setup");
  }

  return options;
}

export function setOptions(newOptions: JobProcessorOptions): void {
  if (options) {
    throw new Error("Job processor options already set");
  }

  options = newOptions;
}
