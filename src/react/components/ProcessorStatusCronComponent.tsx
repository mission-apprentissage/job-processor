import { Alert } from "@codegouvfr/react-dsfr/Alert.js";
import { ProcessorStatusJson } from "../../common/model.ts";
import { Box, CircularProgress } from "@mui/material";
import { fr } from "@codegouvfr/react-dsfr";
import { Accordion } from "@codegouvfr/react-dsfr/Accordion.js";
import { Table, WrappeDataGridAction } from "./Table.tsx";
import { useMemo } from "react";
import { DsfrLink } from "./DsfrLink.tsx";
import { getTaskStatus } from "./ProcessorStatusTaskComponent.tsx";

type ProcessorStatusCronComponentProps = {
  status: ProcessorStatusJson | null;
  baseUrl: string;
  name: string;
};

export function ProcessorStatusCronComponent(
  props: ProcessorStatusCronComponentProps,
) {
  const { status, name } = props;

  const cron = useMemo(() => {
    return status?.crons.find((cron) => cron.cron.name === name);
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

  if (!cron) {
    return <Alert severity="error" title="Non trouvé" />;
  }

  return (
    <>
      <Accordion label="Programmés">
        <Table
          getRowId={(row) => row._id}
          rows={cron.scheduled}
          columns={[
            {
              field: "scheduled_for",
              headerName: "Planifié pour",
              flex: 1,
              type: "dateTime",
              valueGetter: (value) => (value ? new Date(value) : null),
            },
            {
              field: "created_at",
              headerName: "Créé à",
              flex: 1,
              type: "dateTime",
              valueGetter: (value) => (value ? new Date(value) : null),
            },
            {
              field: "actions",
              headerName: "Voir",
              type: "actions",
              getActions: ({ row: { _id } }) => [
                <WrappeDataGridAction key="Voir">
                  <DsfrLink href={`${props.baseUrl}/cron/${name}/${_id}`} />
                </WrappeDataGridAction>,
              ],
            },
          ]}
        />
      </Accordion>
      <Accordion label="En cours">
        <Table
          getRowId={(row) => row._id}
          rows={cron.running}
          columns={[
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
              field: "created_at",
              headerName: "Créé à",
              flex: 1,
              type: "dateTime",
              valueGetter: (value) => (value ? new Date(value) : null),
            },
            {
              field: "actions",
              headerName: "Voir",
              type: "actions",
              getActions: ({ row: { _id } }) => [
                <WrappeDataGridAction key="Voir">
                  <DsfrLink href={`${props.baseUrl}/cron/${name}/${_id}`} />
                </WrappeDataGridAction>,
              ],
            },
          ]}
        />
      </Accordion>
      <Accordion label="Historique">
        <Table
          getRowId={(row) => row._id}
          rows={cron.history}
          columns={[
            {
              field: "status",
              headerName: "Statut",
              flex: 1,
              valueGetter: (_value, row) => getTaskStatus(row),
            },
            {
              field: "started_at",
              headerName: "Démarré à",
              flex: 1,
              type: "dateTime",
              valueGetter: (value) => (value ? new Date(value) : null),
            },
            {
              field: "scheduled_for",
              headerName: "Planifié pour",
              flex: 1,
              type: "dateTime",
              valueGetter: (value) => (value ? new Date(value) : null),
            },
            {
              field: "actions",
              headerName: "Voir",
              type: "actions",
              getActions: ({ row: { _id } }) => [
                <WrappeDataGridAction key="Voir">
                  <DsfrLink href={`${props.baseUrl}/cron/${name}/${_id}`} />
                </WrappeDataGridAction>,
              ],
            },
          ]}
        />
      </Accordion>
    </>
  );
}
