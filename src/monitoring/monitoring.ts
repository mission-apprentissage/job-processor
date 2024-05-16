import { getJobCollection, getWorkerCollection } from "../data/actions.ts";
import {
  IJob,
  IJobsCron,
  IJobsCronTask,
  IJobsSimple,
  IWorker,
  isJobCron,
  isJobCronTask,
  isJobSimple,
  isJobSimpleOrCronTask,
} from "../data/model.ts";

type WorkerStatus = {
  worker: IWorker;
  job: IJobsSimple | IJobsCronTask | null;
};

type QueueStatus = {
  jobs: IJobsSimple[];
  cron_tasks: IJobsCronTask[];
};

type CronStatus = {
  cron: IJobsCron;
  scheduled: IJobsCronTask[];
  running: IJobsCronTask[];
  history: IJobsCronTask[];
};

type JobStatus = {
  name: IJobsSimple["name"];
  scheduled: IJobsSimple[];
  running: IJobsSimple[];
  history: IJobsSimple[];
};

type ProcessorStatus = {
  workers: WorkerStatus[];
  queue: QueueStatus;
  jobs: JobStatus[];
  crons: CronStatus[];
};

function buildWorkerStatus(workers: IWorker[], jobs: IJob[]): WorkerStatus[] {
  return workers.map((worker): WorkerStatus => {
    const job =
      jobs.filter(isJobSimpleOrCronTask).find((job) => {
        return job.worker_id === worker._id;
      }) ?? null;

    return { worker, job };
  });
}

function buildQueueStatus(jobs: IJob[], now: Date): QueueStatus {
  const pending = jobs.filter(
    (job) => job.status === "pending" && job.scheduled_for <= now,
  );

  return {
    jobs: pending.filter(isJobSimple),
    cron_tasks: pending.filter(isJobCronTask),
  };
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
  const names = Array.from(new Set(jobs.map((job) => job.name)));

  return names.map((name) => {
    const instances = jobs
      .filter(isJobSimple)
      .filter((job) => job.name === name);

    const scheduled = instances.filter((job) => job.status === "pending");
    const running = instances.filter(
      (job) => job.status === "running" || job.status === "paused",
    );
    const history = instances.filter(
      (job) => job.status !== "pending" && job.status !== "running",
    );

    return {
      name,
      scheduled,
      running,
      history,
    };
  });
}

export async function getProcessorStatus(): Promise<ProcessorStatus> {
  const now = new Date();
  const [workers, jobs] = await Promise.all([
    getWorkerCollection().find().toArray(),
    getJobCollection().find().toArray(),
  ]);

  return {
    workers: buildWorkerStatus(workers, jobs),
    queue: buildQueueStatus(jobs, now),
    crons: buildCronStatus(jobs),
    jobs: buildJobStatus(jobs),
  };
}
