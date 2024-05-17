import { Box } from "@mui/material";
import { DataGrid, DataGridProps, GridValidRowModel } from "@mui/x-data-grid";
import { frFR } from "@mui/x-data-grid/locales";

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
