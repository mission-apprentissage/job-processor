import { Box } from "@mui/material";
import { DataGrid, DataGridProps, GridValidRowModel } from "@mui/x-data-grid";
import { frFR } from "@mui/x-data-grid/locales";

import type { FC, ReactNode, RefObject } from "react";

export function Table<R extends GridValidRowModel>(props: DataGridProps<R>) {
  return (
    <Box my={2}>
      <DataGrid
        rowHeight={60}
        localeText={frFR.components.MuiDataGrid.defaultProps.localeText}
        {...props}
      />
    </Box>
  );
}

export const WrappeDataGridAction: FC<{ children: ReactNode | undefined; touchRippleRef?: RefObject<unknown> }> = ({
  children,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  touchRippleRef,
}) => {
  return children;
};
