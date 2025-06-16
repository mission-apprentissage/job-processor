import { ObjectId } from "mongodb";
import type {
  Collection,
  Db,
  MongoServerError,
  Filter,
  FindOptions,
  MatchKeysAndValues,
} from "mongodb";
import { zodToMongoSchema } from "zod-mongodb-schema";
import type {
  IJob,
  IJobsCron,
  IJobsCronTask,
  IJobsSimple,
  IWorker,
} from "../../common/model.ts";
import { ZJob } from "../../common/model.ts";
import { getOptions } from "../options.ts";
import { workerId } from "../worker/workerId.ts";

const jobCollectionName = "job_processor.jobs";
const workerCollectionName = "job_processor.workers";

function getDatabase(): Db {
  return getOptions().db;
}

export function getJobCollection(): Collection<IJob> {
  return getDatabase().collection(jobCollectionName);
}

export function getWorkerCollection(): Collection<IWorker> {
  return getDatabase().collection(workerCollectionName);
}

async function executeMigrations() {
  // This migration is light and can safely be executed many times
  // We probably need in future a proper version field stored to manage migrations
  await getJobCollection().updateMany(
    { type: { $in: ["cron_task", "simple"] }, worker_id: { $exists: false } },
    { $set: { worker_id: null } },
    { bypassDocumentValidation: true },
  );
}

async function createIndexes() {
  await Promise.allSettled([
    getJobCollection().createIndexes(
      [
        { key: { type: 1, scheduled_for: 1 } },
        { key: { type: 1, status: 1, scheduled_for: 1 } },
        { key: { type: 1, name: 1, status: 1, scheduled_for: 1 } },
        { key: { type: 1, name: 1 } },
        { key: { status: 1 } },
        {
          key: { ended_at: 1 },
          // 90 days
          expireAfterSeconds: 3600 * 24 * 90,
        },
      ],
      { background: true },
    ),
    getWorkerCollection().createIndexes(
      [{ key: { lastSeen: 1 }, expireAfterSeconds: 300 }],
      { background: true },
    ),
  ]);
}

async function createCollectionIfDoesNotExist(jobCollectionName: string) {
  const db = getDatabase();
  const collectionsInDb = await db.listCollections().toArray();
  const collectionExistsInDb = collectionsInDb
    .map(({ name }) => name)
    .includes(jobCollectionName);

  if (!collectionExistsInDb) {
    try {
      await db.createCollection(jobCollectionName);
    } catch (err) {
      if ((err as MongoServerError).codeName !== "NamespaceExists") {
        throw err;
      }
    }
  }
}

async function configureDbSchemaValidation() {
  const db = getDatabase();
  await createCollectionIfDoesNotExist(jobCollectionName);

  const convertedSchema = zodToMongoSchema(ZJob);

  await db.command({
    collMod: jobCollectionName,
    validationLevel: "strict",
    validationAction: "error",
    validator: {
      $jsonSchema: {
        title: `${jobCollectionName} validation schema`,
        ...convertedSchema,
      },
    },
  });
}

export async function configureDb() {
  await executeMigrations();
  await configureDbSchemaValidation();
  await createIndexes();
}

type CreateJobSimpleParams = Pick<
  IJobsSimple,
  "name" | "payload" | "scheduled_for" | "sync"
>;

export const createJobSimple = async ({
  name,
  payload,
  scheduled_for,
  sync,
}: CreateJobSimpleParams): Promise<IJobsSimple> => {
  const now = new Date();

  const job: IJobsSimple = {
    _id: new ObjectId(),
    name,
    type: "simple",
    status: sync ? "running" : "pending",
    payload,
    updated_at: now,
    created_at: now,
    scheduled_for,
    sync,
    worker_id: null,
  };
  await getJobCollection().insertOne(job);
  return job;
};

type CreateJobCronParams = Pick<
  IJobsCron,
  "name" | "cron_string" | "scheduled_for"
>;

export const createJobCron = async ({
  name,
  cron_string,
  scheduled_for,
}: CreateJobCronParams): Promise<IJobsCron> => {
  const now = new Date();

  const job: IJobsCron = {
    _id: new ObjectId(),
    name,
    type: "cron",
    status: "active",
    cron_string,
    updated_at: now,
    created_at: now,
    scheduled_for,
  };
  await getJobCollection().insertOne(job);
  return job;
};

type CreateJobCronTaskParams = Pick<IJobsCron, "name" | "scheduled_for">;

export const createJobCronTask = async ({
  name,
  scheduled_for,
}: CreateJobCronTaskParams): Promise<IJobsCronTask> => {
  const now = new Date();

  const job: IJobsCronTask = {
    _id: new ObjectId(),
    name,
    type: "cron_task",
    status: "pending",
    updated_at: now,
    created_at: now,
    started_at: null,
    ended_at: null,
    scheduled_for,
    worker_id: null,
  };
  await getJobCollection().insertOne(job);
  return job;
};

export const findJobCron = async (
  filter: Filter<IJobsCron>,
  options?: FindOptions,
): Promise<IJobsCron | null> => {
  const f: Filter<IJobsCron> = { type: "cron", ...filter };
  // @ts-expect-error typescript cannot refine union documents
  return await getJobCollection().findOne<IJobsCron>(f, options);
};

export const getSimpleJob = async (
  id: ObjectId,
): Promise<IJobsSimple | null> => {
  return await getJobCollection().findOne<IJobsSimple>({
    _id: id,
    type: "simple",
  });
};

export const getCronTaskJob = async (
  id: ObjectId,
): Promise<IJobsCronTask | null> => {
  return await getJobCollection().findOne<IJobsCronTask>({
    _id: id,
    type: "cron_task",
  });
};

export const findSimpleJob = async (
  filter: Filter<IJobsSimple>,
  options?: FindOptions,
): Promise<IJobsSimple | null> => {
  const f: Filter<IJobsSimple> = { type: "simple", ...filter };
  // @ts-expect-error typescript cannot refine union documents
  return await getJobCollection().findOne<IJobsSimple>(f, options);
};

export const findJobs = async <T extends IJob>(
  filter: Filter<T>,
  options?: FindOptions,
): Promise<T[]> => {
  // @ts-expect-error typescript cannot refine union documents
  return await getJobCollection().find(filter, options).toArray();
};

export const updateJob = async (
  _id: ObjectId,
  data: MatchKeysAndValues<IJob>,
) => {
  return getJobCollection().updateOne(
    { _id },
    { $set: { ...data, updated_at: new Date() } },
  );
};

function getWorkerScopeJob(): Filter<IJob> {
  const options = getOptions();
  const workerTags = options.workerTags ?? null;

  if (workerTags === null) {
    return {
      type: { $in: ["simple", "cron_task"] },
    };
  }

  const jobNames = Object.entries(options.jobs)
    .filter(([, def]) => {
      const tag = def.tag ?? null;
      if (tag === null) {
        return true;
      }

      return workerTags.includes(tag);
    })
    .map(([name]) => name);

  const taskNames = Object.entries(options.crons)
    .filter(([, def]) => {
      const tag = def.tag ?? null;
      if (tag === null) {
        return true;
      }

      return workerTags.includes(tag);
    })
    .map(([name]) => name);

  return {
    $or: [
      { type: "simple", name: { $in: jobNames } },
      { type: "cron_task", name: { $in: taskNames } },
    ],
  };
}

export async function pickNextJob(): Promise<
  IJobsCronTask | IJobsSimple | null
> {
  return getJobCollection().findOneAndUpdate(
    {
      $and: [
        getWorkerScopeJob(),
        {
          status: { $in: ["paused", "pending"] },
          scheduled_for: { $lte: new Date() },
        },
      ],
    },
    [
      {
        $set: {
          status: "running",
          worker_id: workerId,
          started_at: { $ifNull: ["$started_at", new Date()] },
        },
      },
    ],
    { sort: { scheduled_for: 1 }, includeResultMetadata: false },
  ) as Promise<IJobsCronTask | IJobsSimple | null>;
}

export async function detectExitedJobs(): Promise<
  IJobsCronTask | IJobsSimple | null
> {
  const now = new Date();
  const activeWorkerIds = await getWorkerCollection()
    .find({}, { projection: { _id: 1 } })
    .toArray();

  // Any job started more than 5min ago is garanted to be in active worker if not dead
  // We need to be careful in case a worker register just after we get active workers
  return getJobCollection().findOneAndUpdate(
    {
      type: { $in: ["simple", "cron_task"] },
      status: "running",
      worker_id: { $nin: activeWorkerIds.map(({ _id }) => _id) },
      started_at: { $lt: new Date(now.getTime() - 300_000) },
    },
    {
      $set: {
        status: "errored",
        output: {
          duration: "--",
          result: null,
          error: "Worker crashed unexpectly",
        },
        ended_at: now,
      },
    },
    {
      returnDocument: "after",
      includeResultMetadata: false,
    },
  ) as Promise<IJobsCronTask | IJobsSimple | null>;
}
