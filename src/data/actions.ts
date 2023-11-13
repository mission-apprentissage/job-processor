import { Filter, FindOptions, MatchKeysAndValues, ObjectId } from "mongodb";
import { IJob, IJobsCron, IJobsCronTask, IJobsSimple, ZJob } from "./model.ts";
import type { Collection, Db, MongoServerError } from "mongodb";
import { getOptions } from "../setup.ts";
import { zodToMongoSchema } from "zod-mongodb-schema";

const collectionName = "job_processor.jobs";

function getDatabase(): Db {
  return getOptions().db;
}

export function getJobCollection(): Collection<IJob> {
  return getDatabase().collection(collectionName);
}

async function createCollectionIfDoesNotExist(collectionName: string) {
  const db = getDatabase();
  const collectionsInDb = await db.listCollections().toArray();
  const collectionExistsInDb = collectionsInDb
    .map(({ name }) => name)
    .includes(collectionName);

  if (!collectionExistsInDb) {
    try {
      await db.createCollection(collectionName);
    } catch (err) {
      if ((err as MongoServerError).codeName !== "NamespaceExists") {
        throw err;
      }
    }
  }
}

export async function configureDbSchemaValidation() {
  const db = getDatabase();
  await createCollectionIfDoesNotExist(collectionName);

  const convertedSchema = zodToMongoSchema(ZJob);

  await db.command({
    collMod: collectionName,
    validationLevel: "strict",
    validationAction: "error",
    validator: {
      $jsonSchema: {
        title: `${collectionName} validation schema`,
        ...convertedSchema,
      },
    },
  });
}

type CreateJobSimpleParams = Pick<
  IJobsSimple,
  "name" | "payload" | "scheduled_for" | "sync"
>;

export const createJobSimple = async ({
  name,
  payload,
  scheduled_for = new Date(),
  sync = false,
}: CreateJobSimpleParams): Promise<IJobsSimple> => {
  const job: IJobsSimple = {
    _id: new ObjectId(),
    name,
    type: "simple",
    status: sync ? "will_start" : "pending",
    payload,
    updated_at: new Date(),
    created_at: new Date(),
    scheduled_for,
    sync,
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
  scheduled_for = new Date(),
}: CreateJobCronParams): Promise<IJobsCron> => {
  const job: IJobsCron = {
    _id: new ObjectId(),
    name,
    type: "cron",
    status: "active",
    cron_string,
    updated_at: new Date(),
    created_at: new Date(),
    scheduled_for,
  };
  await getJobCollection().insertOne(job);
  return job;
};

export const updateJobCron = async (
  id: ObjectId,
  cron_string: IJobsCron["cron_string"],
): Promise<void> => {
  const data: Partial<IJobsCron> = {
    status: "active",
    cron_string,
    updated_at: new Date(),
  };

  await getJobCollection().findOneAndUpdate(id, data);
};

type CreateJobCronTaskParams = Pick<IJobsCron, "name" | "scheduled_for">;

export const createJobCronTask = async ({
  name,
  scheduled_for,
}: CreateJobCronTaskParams): Promise<IJobsCronTask> => {
  const job: IJobsCronTask = {
    _id: new ObjectId(),
    name,
    type: "cron_task",
    status: "pending",
    updated_at: new Date(),
    created_at: new Date(),
    scheduled_for,
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
  return await getJobCollection().findOne(f, options);
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

export function pickNextJob(): Promise<IJobsCronTask | IJobsSimple | null> {
  return getJobCollection().findOneAndUpdate(
    {
      type: { $in: ["simple", "cron_task"] },
      status: "pending",
      scheduled_for: { $lte: new Date() },
    },
    { $set: { status: "will_start" } },
    { sort: { scheduled_for: 1 }, includeResultMetadata: false },
  ) as Promise<IJobsCronTask | IJobsSimple | null>;
}
