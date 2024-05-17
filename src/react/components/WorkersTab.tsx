import { Accordion } from "@codegouvfr/react-dsfr/Accordion.js";

import { Table } from "./Table.tsx";
import { useMemo } from "react";
import { ProcessorStatusJson } from "../../monitoring/monitoring.ts";

export function WorkersTab(
  props: Pick<ProcessorStatusJson, "workers" | "queue">,
) {
  const workerRows = useMemo(() => {
    return props.workers.map((worker) => {
      return {
        _id: worker.worker._id,
        hostname: worker.worker.hostname,
        lastSeen: worker.worker.lastSeen
          ? new Date(worker.worker.lastSeen)
          : null,
        taskName: worker.task?.name,
        taskStartedAt: worker.task?.started_at
          ? new Date(worker.task.started_at)
          : null,
      };
    });
  }, [props.workers]);

  return (
    <>
      <Accordion label="Unités de travail">
        <Table
          rows={workerRows}
          getRowId={(worker) => worker._id}
          columns={[
            {
              field: "_id",
              headerName: "Identifiant",
              flex: 1,
            },
            {
              field: "hostname",
              headerName: "Hôte",
              flex: 1,
            },
            {
              field: "lastSeen",
              headerName: "Dernière vue",
              flex: 1,
              type: "dateTime",
            },
            {
              field: "taskName",
              headerName: "Tâche en cours",
              flex: 1,
            },
            {
              field: "taskStartedAt",
              headerName: "Démarrée le",
              flex: 1,
              type: "dateTime",
            },
          ]}
        />
      </Accordion>
      <Accordion label="File d'attente">
        <Table
          rows={props.queue}
          columns={[
            {
              field: "status",
              headerName: "Statut",
              flex: 1,
            },
            {
              field: "name",
              headerName: "Nom",
              flex: 1,
            },
            {
              field: "scheduled_for",
              headerName: "Planifié pour",
              flex: 1,
              type: "dateTime",
              valueGetter: ({ value }) => (value ? new Date(value) : null),
            },
          ]}
        />
      </Accordion>
    </>
  );
}
