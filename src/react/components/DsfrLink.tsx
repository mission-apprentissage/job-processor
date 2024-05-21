import { fr } from "@codegouvfr/react-dsfr";
import { Link, LinkProps } from "@mui/material";

export function DsfrLink({ children, ...props }: LinkProps) {
  return (
    <Link
      sx={{ textUnderlinePosition: "under" }}
      className={fr.cx(
        `fr-text--md`,
        "fr-link--icon-right",
        "fr-icon-arrow-right-line",
      )}
      {...props}
    >
      {children}
    </Link>
  );
}
