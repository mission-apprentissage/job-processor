import type { ILogger } from "./setup.ts";
import { getOptions } from "./options.ts";

export function getLogger(): ILogger {
  return getOptions().logger;
}
