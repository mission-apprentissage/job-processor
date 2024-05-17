import { Alert } from "@codegouvfr/react-dsfr/Alert.js";
import { ProcessorStatusJson, IJobsSimple } from "../../common/model.ts";
import { Box, CircularProgress } from "@mui/material";
import { fr } from "@codegouvfr/react-dsfr";
import { Table } from "./Table.tsx";
import { useMemo } from "react";
import { DsfrLink } from "./DsfrLink.tsx";
import { getTaskStatus } from "./ProcessorStatusTaskComponent.tsx";
import type { Jsonify } from "type-fest";

type ProcessorStatusJobComponentProps = {
  status: ProcessorStatusJson | null;
  baseUrl: string;
  name: string;
};

export function ProcessorStatusJobComponent(
  props: ProcessorStatusJobComponentProps,
) {
  const { status, name } = props;

  const job = useMemo(() => {
    return status?.jobs.find((job) => job.name === name);
  }, [name, status]);

  if (!status) {
    return (
      <Box
        sx={{
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          my: fr.spacing("8w"),
        }}
      >
        <CircularProgress />
      </Box>
    );
  }

  if (!job) {
    return <Alert severity="error" title="Non trouvé" />;
  }

  return (
    <Table
      getRowId={(row) => row._id}
      rows={job.tasks}
      columns={[
        {
          field: "status",
          headerName: "Statut",
          flex: 1,
          valueGetter: (_value, row) => getTaskStatus(row),
        },
        {
          field: "scheduled_for",
          headerName: "Planifié pour",
          flex: 1,
          type: "dateTime",
          valueGetter: (value) => (value ? new Date(value) : null),
        },
        {
          field: "started_at",
          headerName: "Démarré à",
          flex: 1,
          type: "dateTime",
          valueGetter: (value) => (value ? new Date(value) : null),
        },
        {
          field: "ended_at",
          headerName: "Fini à",
          flex: 1,
          type: "dateTime",
          valueGetter: (value) => (value ? new Date(value) : null),
        },
        {
          field: "output.duration",
          headerName: "Durée",
          flex: 1,
          valueGetter: ({ row }: { row: Jsonify<IJobsSimple> }) =>
            row.output?.duration ?? null,
        },
        {
          field: "actions",
          headerName: "Voir",
          type: "actions",
          getActions: ({ row: { _id } }) => [
            <DsfrLink
              key="Voir"
              href={`${props.baseUrl}/job/${name}/${_id}`}
            />,
          ],
        },
      ]}
    />
  );
}
