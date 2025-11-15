import { beforeAll, describe, expect, it, vi } from "vitest";
import { MongoClient } from "mongodb";
import { initJobProcessor } from "../setup.ts";
import { processorEventEmitter, runJobProcessor } from "./processor.ts";

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
      sleepTimeWhenNoJobsInMs: 1000, // 1 second for faster test
    });

    return async () => {
      await client?.close();
    };
  });

  it("should use custom sleep time when configured", async () => {
    const ctrl = new AbortController();
    const startTime = Date.now();

    // Start the processor - it should sleep when there are no jobs
    const teardown = runJobProcessor(ctrl.signal);

    // Wait for the processor to enter sleep (no jobs in queue)
    const onContinue = new Promise((resolve) =>
      processorEventEmitter.once("continue", resolve),
    );

    await onContinue;
    const elapsedTime = Date.now() - startTime;

    // The processor should have slept for approximately 1000ms
    // Allow some tolerance for execution time
    expect(elapsedTime).toBeGreaterThanOrEqual(900);
    expect(elapsedTime).toBeLessThan(1500);

    ctrl.abort();
    await teardown;
  });
});
