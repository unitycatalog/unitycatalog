import { Link } from "@tanstack/react-router";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";

// Shared catalog > schema > leaf breadcrumb trail used by every detail page.
// `leaf` is the current object's name (rendered as the non-link current page).
export default function CatalogCrumbs({
  catalog,
  schema,
  leaf,
}: {
  catalog?: string;
  schema?: string;
  leaf?: string;
}) {
  return (
    <Breadcrumb>
      <BreadcrumbList>
        <BreadcrumbItem>
          <BreadcrumbLink asChild>
            <Link to="/">Catalog</Link>
          </BreadcrumbLink>
        </BreadcrumbItem>
        {catalog && (
          <>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              {schema || leaf ? (
                <BreadcrumbLink asChild>
                  <Link to="/catalog/$catalog" params={{ catalog }}>
                    {catalog}
                  </Link>
                </BreadcrumbLink>
              ) : (
                <BreadcrumbPage>{catalog}</BreadcrumbPage>
              )}
            </BreadcrumbItem>
          </>
        )}
        {catalog && schema && (
          <>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              {leaf ? (
                <BreadcrumbLink asChild>
                  <Link to="/catalog/$catalog/$schema" params={{ catalog, schema }}>
                    {schema}
                  </Link>
                </BreadcrumbLink>
              ) : (
                <BreadcrumbPage>{schema}</BreadcrumbPage>
              )}
            </BreadcrumbItem>
          </>
        )}
        {leaf && (
          <>
            <BreadcrumbSeparator />
            <BreadcrumbItem>
              <BreadcrumbPage>{leaf}</BreadcrumbPage>
            </BreadcrumbItem>
          </>
        )}
      </BreadcrumbList>
    </Breadcrumb>
  );
}
