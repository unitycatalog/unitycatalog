import { useEffect, useState } from "react";
import { Link, useParams } from "@tanstack/react-router";
import {
  Box,
  ChartNoAxesCombined,
  ChevronDown,
  ChevronRight,
  Database,
  FunctionSquare,
  HardDrive,
  Notebook,
  Table as TableIcon,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { useListCatalogs } from "@/hooks/catalog";
import { useListFunctions } from "@/hooks/functions";
import { useListModels } from "@/hooks/models";
import { useListSchemas } from "@/hooks/schemas";
import { useListTables } from "@/hooks/tables";
import { useListViews } from "@/hooks/views";
import { useListVolumes } from "@/hooks/volumes";
import { cn } from "@/lib/utils";

type RouteParams = {
  catalog?: string;
  schema?: string;
  table?: string;
  view?: string;
  volume?: string;
  function?: string;
  model?: string;
};

// CatalogTree is the persistent left-hand navigation: a lazily expanded
// catalog > schema tree where each schema fans out into grouped securable
// sections (Tables, Volumes, Functions, Models). Nodes deep-link to detail
// routes; the active node + expansion are derived from the current route params.
export default function CatalogTree() {
  const params = useParams({ strict: false }) as RouteParams;
  const catalogs = useListCatalogs();

  return (
    <div>
      {catalogs.isLoading && (
        <p className="px-2 py-1 text-xs text-muted-foreground">Loading…</p>
      )}
      {!!catalogs.error && (
        <p className="px-2 py-1 text-xs text-destructive">
          {catalogs.error.message}
        </p>
      )}
      {catalogs.isSuccess && (catalogs.data.catalogs ?? []).length === 0 && (
        <p className="px-2 py-1 text-xs text-muted-foreground">No catalogs.</p>
      )}
      <ul className="space-y-0.5">
        {(catalogs.data?.catalogs ?? []).map((c) => (
          <CatalogNode key={c.name} catalog={c.name ?? ""} params={params} />
        ))}
      </ul>
    </div>
  );
}

const rowCls = (active: boolean) =>
  cn(
    "flex items-center gap-0.5 rounded-md",
    active ? "bg-accent text-accent-foreground" : "hover:bg-accent/60",
  );

const linkCls = (active: boolean) =>
  cn(
    "flex min-w-0 flex-1 items-center gap-1.5 rounded-md px-1.5 py-1 text-left",
    active ? "font-medium" : "text-foreground",
  );

function Chevron({ open, onClick }: { open: boolean; onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-label={open ? "Collapse" : "Expand"}
      className="shrink-0 rounded p-0.5 text-muted-foreground hover:bg-accent hover:text-foreground"
    >
      {open ? (
        <ChevronDown className="h-3.5 w-3.5" />
      ) : (
        <ChevronRight className="h-3.5 w-3.5" />
      )}
    </button>
  );
}

function CatalogNode({
  catalog,
  params,
}: {
  catalog: string;
  params: RouteParams;
}) {
  const [open, setOpen] = useState(params.catalog === catalog);
  useEffect(() => {
    if (params.catalog === catalog) setOpen(true);
  }, [params.catalog, catalog]);

  const active = params.catalog === catalog && !params.schema;
  const schemas = useListSchemas(catalog, open);

  return (
    <li>
      <div className={rowCls(active)}>
        <Chevron open={open} onClick={() => setOpen((o) => !o)} />
        <Link
          to="/catalog/$catalog"
          params={{ catalog }}
          onClick={() => setOpen(true)}
          className={linkCls(active)}
        >
          <Notebook className="h-4 w-4 shrink-0 text-chart-1" />
          <span className="truncate">{catalog}</span>
        </Link>
      </div>
      {open && (
        <ul className="ml-3.5 mt-0.5 space-y-0.5 border-l pl-1.5">
          {schemas.isLoading && (
            <li className="px-2 py-1 text-xs text-muted-foreground">
              Loading…
            </li>
          )}
          {!!schemas.error && (
            <li className="px-2 py-1 text-xs text-destructive">
              {schemas.error.message}
            </li>
          )}
          {schemas.isSuccess && (schemas.data.schemas ?? []).length === 0 && (
            <li className="px-2 py-0.5 text-xs text-muted-foreground">
              No schemas.
            </li>
          )}
          {(schemas.data?.schemas ?? []).map((s) => (
            <SchemaNode
              key={s.name}
              catalog={catalog}
              schema={s.name ?? ""}
              params={params}
            />
          ))}
        </ul>
      )}
    </li>
  );
}

function SchemaNode({
  catalog,
  schema,
  params,
}: {
  catalog: string;
  schema: string;
  params: RouteParams;
}) {
  const here = params.catalog === catalog && params.schema === schema;
  const [open, setOpen] = useState(here);
  useEffect(() => {
    if (here) setOpen(true);
  }, [here]);

  const active =
    here &&
    !params.table &&
    !params.view &&
    !params.volume &&
    !params.function &&
    !params.model;

  return (
    <li>
      <div className={rowCls(active)}>
        <Chevron open={open} onClick={() => setOpen((o) => !o)} />
        <Link
          to="/catalog/$catalog/$schema"
          params={{ catalog, schema }}
          onClick={() => setOpen(true)}
          className={cn(linkCls(active), "text-xs")}
        >
          <Database className="h-3.5 w-3.5 shrink-0 text-chart-4" />
          <span className="truncate">{schema}</span>
        </Link>
      </div>
      {open && (
        <ul className="ml-3.5 mt-0.5 space-y-0.5 border-l pl-1.5">
          <SchemaGroup
            kind="tables"
            catalog={catalog}
            schema={schema}
            params={params}
            defaultOpen
          />
          <SchemaGroup
            kind="views"
            catalog={catalog}
            schema={schema}
            params={params}
          />
          <SchemaGroup
            kind="volumes"
            catalog={catalog}
            schema={schema}
            params={params}
          />
          <SchemaGroup
            kind="functions"
            catalog={catalog}
            schema={schema}
            params={params}
          />
          <SchemaGroup
            kind="models"
            catalog={catalog}
            schema={schema}
            params={params}
          />
        </ul>
      )}
    </li>
  );
}

type GroupKind = "tables" | "views" | "volumes" | "functions" | "models";
type Named = { name: string };

const GROUP_META: Record<
  GroupKind,
  { label: string; Icon: LucideIcon; color: string }
> = {
  tables: { label: "Tables", Icon: TableIcon, color: "text-chart-2" },
  views: {
    label: "Metric views",
    Icon: ChartNoAxesCombined,
    color: "text-chart-5",
  },
  volumes: { label: "Volumes", Icon: HardDrive, color: "text-chart-4" },
  functions: {
    label: "Functions",
    Icon: FunctionSquare,
    color: "text-chart-3",
  },
  models: { label: "Models", Icon: Box, color: "text-chart-1" },
};

const GROUP_ROUTE_PARAM: Record<GroupKind, keyof RouteParams> = {
  tables: "table",
  views: "view",
  volumes: "volume",
  functions: "function",
  models: "model",
};

function SchemaGroup({
  kind,
  catalog,
  schema,
  params,
  defaultOpen = false,
}: {
  kind: GroupKind;
  catalog: string;
  schema: string;
  params: RouteParams;
  defaultOpen?: boolean;
}) {
  const activeName = params[GROUP_ROUTE_PARAM[kind]];
  const routeOpen =
    params.catalog === catalog && params.schema === schema && !!activeName;
  const [open, setOpen] = useState(defaultOpen || routeOpen);
  useEffect(() => {
    if (routeOpen) setOpen(true);
  }, [routeOpen]);
  const meta = GROUP_META[kind];
  const tables = useListTables(catalog, schema, open && kind === "tables");
  const views = useListViews(catalog, schema, open && kind === "views");
  const volumes = useListVolumes(catalog, schema, open && kind === "volumes");
  const functions = useListFunctions(
    catalog,
    schema,
    open && kind === "functions",
  );
  const models = useListModels(catalog, schema, open && kind === "models");
  const result = { tables, views, volumes, functions, models }[kind];
  const items: Named[] =
    kind === "tables"
      ? (tables.data?.tables ?? [])
      : kind === "views"
        ? (views.data?.views ?? [])
        : kind === "volumes"
          ? (volumes.data?.volumes ?? [])
          : kind === "functions"
            ? (functions.data?.functions ?? [])
            : (models.data?.models ?? []);
  const count = result.isSuccess ? items.length : undefined;

  return (
    <li>
      <div className="flex items-center gap-0.5 rounded-md hover:bg-accent/60">
        <Chevron open={open} onClick={() => setOpen((o) => !o)} />
        <button
          type="button"
          onClick={() => setOpen((o) => !o)}
          className="flex min-w-0 flex-1 items-center gap-1.5 rounded-md px-1.5 py-0.5 text-left text-xs font-medium text-muted-foreground"
        >
          <meta.Icon className={cn("h-3.5 w-3.5 shrink-0", meta.color)} />
          <span className="truncate">
            {meta.label}
            {count !== undefined ? ` (${count})` : ""}
          </span>
        </button>
      </div>
      {open && (
        <ul className="ml-3.5 mt-0.5 space-y-0.5 border-l pl-1.5">
          {result.isLoading && (
            <li className="px-2 py-0.5 text-xs text-muted-foreground">
              Loading…
            </li>
          )}
          {!!result.error && (
            <li className="px-2 py-0.5 text-xs text-destructive">
              {result.error.message}
            </li>
          )}
          {result.isSuccess && items.length === 0 && (
            <li className="px-2 py-0.5 text-xs text-muted-foreground">None</li>
          )}
          {items.map((it) => (
            <GroupItem
              key={it.name}
              kind={kind}
              catalog={catalog}
              schema={schema}
              name={it.name ?? ""}
              active={activeName === it.name}
            />
          ))}
        </ul>
      )}
    </li>
  );
}

const itemCls =
  "flex items-center gap-1.5 rounded-md px-1.5 py-0.5 text-xs text-muted-foreground hover:bg-accent/60 hover:text-foreground";

function GroupItem({
  kind,
  catalog,
  schema,
  name,
  active,
}: {
  kind: GroupKind;
  catalog: string;
  schema: string;
  name: string;
  active: boolean;
}) {
  const meta = GROUP_META[kind];
  const icon = <meta.Icon className={cn("h-3.5 w-3.5 shrink-0", meta.color)} />;

  if (kind === "tables") {
    return (
      <li>
        <Link
          to="/catalog/$catalog/$schema/table/$table"
          params={{ catalog, schema, table: name }}
          className={cn(itemCls, active && "bg-accent text-accent-foreground")}
        >
          {icon}
          <span className="truncate">{name}</span>
        </Link>
      </li>
    );
  }
  if (kind === "volumes") {
    return (
      <li>
        <Link
          to="/catalog/$catalog/$schema/volume/$volume"
          params={{ catalog, schema, volume: name }}
          className={cn(itemCls, active && "bg-accent text-accent-foreground")}
        >
          {icon}
          <span className="truncate">{name}</span>
        </Link>
      </li>
    );
  }
  if (kind === "views") {
    return (
      <li>
        <Link
          to="/catalog/$catalog/$schema/view/$view"
          params={{ catalog, schema, view: name }}
          className={cn(itemCls, active && "bg-accent text-accent-foreground")}
        >
          {icon}
          <span className="truncate">{name}</span>
        </Link>
      </li>
    );
  }
  if (kind === "functions") {
    return (
      <li>
        <Link
          to="/catalog/$catalog/$schema/function/$function"
          params={{ catalog, schema, function: name }}
          className={cn(itemCls, active && "bg-accent text-accent-foreground")}
        >
          {icon}
          <span className="truncate">{name}</span>
        </Link>
      </li>
    );
  }
  return (
    <li>
      <Link
        to="/catalog/$catalog/$schema/model/$model"
        params={{ catalog, schema, model: name }}
        className={cn(itemCls, active && "bg-accent text-accent-foreground")}
      >
        {icon}
        <span className="truncate">{name}</span>
      </Link>
    </li>
  );
}
