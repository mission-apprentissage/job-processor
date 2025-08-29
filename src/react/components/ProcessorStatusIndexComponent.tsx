import { Box, CircularProgress } from "@mui/material";
import { Tabs } from "@codegouvfr/react-dsfr/Tabs.js";
import { fr } from "@codegouvfr/react-dsfr";
import type { ProcessorStatusJson } from "../../common/model.ts";
import { WorkersTab } from "./WorkersTab.tsx";
import { JobsTab } from "./JobsTab.tsx";
import { CronTab } from "./CronTab.tsx";

type ProcessorStatusIndexComponentProps = {
  status: ProcessorStatusJson | null;
  baseUrl: string;
};

export function ProcessorStatusIndexComponent(
  props: ProcessorStatusIndexComponentProps,
) {
  const { status, baseUrl } = props;

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

  return (
    <Tabs
      tabs={[
        {
          label: `Workers`,
          content: <WorkersTab {...status} />,
        },
        { label: `Jobs`, content: <JobsTab {...status} baseUrl={baseUrl} /> },
        { label: `CRONs`, content: <CronTab {...status} baseUrl={baseUrl} /> },
      ]}
    />
  );
}
