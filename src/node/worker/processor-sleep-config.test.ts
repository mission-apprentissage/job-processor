import {
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import { MongoClient } from "mongodb";
import { initJobProcessor } from "../setup.ts";
import * as sleepModule from "../../utils/sleep.ts";
import { runJobProcessor } from "./processor.ts";

describe("runJobProcessor - custom sleep time configuration", () => {
  let client: MongoClient | null;

  beforeAll(async () => {
    client = new MongoClient(
      `mongodb://127.0.0.1:27019/${process.env["VITEST_POOL_ID"]}`,
    );
    await client.connect();
    const logger = {
      debug: vi.fn() as any,
      info: vi.fn() as any,
      error: vi.fn() as any,
      child: () => logger,
    };

    // Initialize with custom sleep time
    await initJobProcessor({
      logger,
      db: client.db(),
      jobs: {
        test: {
          handler: vi.fn(),
        },
      },
      crons: {},
      sleepTimeWhenNoJobsInMs: 1000,
    });

    return async () => {
      await client?.close();
    };
  });

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("should use custom sleep time when configured", async () => {
    const ctrl = new AbortController();
    const sleepSpy = vi.spyOn(sleepModule, "sleep");

    // Start the processor - it should sleep when there are no jobs
    const processorPromise = runJobProcessor(ctrl.signal);

    // Wait for the processor to call sleep
    await vi.waitFor(
      () => {
        expect(sleepSpy).toHaveBeenCalledWith(1000, ctrl.signal);
      },
      { timeout: 100 },
    );

    ctrl.abort();
    await processorPromise;

    sleepSpy.mockRestore();
  });
});
