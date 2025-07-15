import { useMemo } from "react";

import type { ProcessorStatusJson } from "../../common/model.ts";
import { getTaskStatus } from "./ProcessorStatusTaskComponent.tsx";
import { Table } from "./Table.tsx";
import { DsfrLink } from "./DsfrLink.tsx";

export function CronTab(
  props: Pick<ProcessorStatusJson, "crons"> & { baseUrl: string },
) {
  const rows = useMemo(() => {
    return props.crons.map(({ cron, scheduled, running, history }) => {
      const lastRun = running[0] ?? history[0];
      const nextRun = scheduled[0];

      return {
        _id: cron.name,
        name: cron.name,
        cron_string: cron.cron_string,
        scheduled_for: nextRun == null ? null : new Date(nextRun.scheduled_for),
        last_execution_status: getTaskStatus(lastRun),
        last_execution_duration: lastRun?.output?.duration ?? "-",
        last_execution_date: lastRun?.started_at
          ? new Date(lastRun.started_at)
          : null,
      };
    });
  }, [props.crons]);

  return (
    <Table
      getRowId={(row) => row._id}
      rows={rows}
      columns={[
        {
          field: "name",
          headerName: "Nom",
          flex: 1,
        },
        {
          field: "cron_string",
          headerName: "Programmation",
          flex: 1,
        },
        {
          field: "scheduled_for",
          headerName: "Planifié pour",
          type: "dateTime",
        },
        {
          field: "last_execution_status",
          headerName: "Dernier Statut",
        },
        {
          field: "last_execution_duration",
          headerName: "Durée",
        },
        {
          field: "last_execution_date",
          headerName: "Date de la dernière exécution",
          type: "dateTime",
        },
        {
          field: "actions",
          headerName: "Voir",
          type: "actions",
          getActions: ({ row: { name } }) => [
            <DsfrLink key="Voir" href={`${props.baseUrl}/cron/${name}`} />,
          ],
        },
      ]}
    />
  );
}
