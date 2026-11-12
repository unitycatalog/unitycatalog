import { useRef, useState } from "react";
import { Plus, X } from "lucide-react";
import { ColumnTypeName } from "@/gen/uc/v1/common_pb";
import { DataSourceFormat } from "@/gen/uc/v1/table_pb";
import { VolumeType } from "@/gen/uc/v1/volume_pb";
import { useCreateFunction } from "@/hooks/functions";
import { useCreateModel } from "@/hooks/models";
import { useCreateTable } from "@/hooks/tables";
import { useCreateView } from "@/hooks/views";
import { useCreateVolume } from "@/hooks/volumes";
import EntityFormDialog from "@/components/EntityFormDialog";
import FormField from "@/components/FormField";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

type ObjectKind = "table" | "volume" | "function" | "model" | "view";

const scalarTypes = [
  ColumnTypeName.BOOLEAN,
  ColumnTypeName.INT,
  ColumnTypeName.LONG,
  ColumnTypeName.DOUBLE,
  ColumnTypeName.DATE,
  ColumnTypeName.TIMESTAMP,
  ColumnTypeName.STRING,
  ColumnTypeName.BINARY,
  ColumnTypeName.VARIANT,
];

export default function CreateSchemaObjectAction({
  catalog,
  schema,
}: {
  catalog: string;
  schema: string;
}) {
  const [kind, setKind] = useState<ObjectKind>("table");
  const [volumeType, setVolumeType] = useState(VolumeType.MANAGED);
  const createTable = useCreateTable();
  const createVolume = useCreateVolume();
  const createFunction = useCreateFunction();
  const createModel = useCreateModel();
  const createView = useCreateView();
  const create = (form: FormData) => {
    const value = (field: string) => String(form.get(field) ?? "");
    const comment = value("comment") || undefined;
    const columnTypes = form.getAll("columnType");
    const columns = form.getAll("column").map((name, index) => ({
      name: String(name),
      typeName: Number(columnTypes[index]) as ColumnTypeName,
      nullable: true,
    }));
    const reference = {
      catalogName: catalog,
      schemaName: schema,
      name: value("name"),
    };
    switch (kind) {
      case "table":
        return createTable.mutateAsync({
          table: reference,
          dataSourceFormat: Number(value("format")) as DataSourceFormat,
          columns,
          storageLocation: { value: value("location") },
          comment,
        });
      case "volume":
        return createVolume.mutateAsync({
          volume: reference,
          volumeType,
          storageLocation:
            volumeType === VolumeType.EXTERNAL
              ? { value: value("location") }
              : undefined,
          comment,
        });
      case "function":
        return createFunction.mutateAsync({
          function: reference,
          returnType: Number(value("returnType")) as ColumnTypeName,
          routineDefinition: value("definition"),
          comment,
        });
      case "model":
        return createModel.mutateAsync({
          model: reference,
          comment,
        });
      case "view":
        return createView.mutateAsync({
          view: reference,
          columns,
          viewDefinition: value("definition"),
          dependencies: [
            {
              target: {
                case: "tableFullName",
                value: value("dependency"),
              },
            },
          ],
          comment,
        });
    }
  };

  return (
    <EntityFormDialog
      title={`Create ${kind}`}
      triggerLabel="Create object"
      onSubmit={create}
    >
      <FormField id="object-kind" label="Object type">
        <select
          id="object-kind"
          className="h-9 w-full rounded-md border bg-transparent px-3 text-sm"
          value={kind}
          onChange={(event) => setKind(event.target.value as ObjectKind)}
        >
          <option value="table">External table</option>
          <option value="volume">Volume</option>
          <option value="function">Function</option>
          <option value="model">Registered model</option>
          <option value="view">Metric view</option>
        </select>
      </FormField>
      <FormField id="object-name" label="Name">
        <Input id="object-name" name="name" required />
      </FormField>
      {(kind === "table" || kind === "view") && <ColumnRows />}
      {kind === "table" && (
        <FormField id="table-format" label="Data source format">
          <select
            id="table-format"
            name="format"
            className="h-9 w-full rounded-md border bg-transparent px-3 text-sm"
            defaultValue={DataSourceFormat.DELTA}
          >
            {[
              DataSourceFormat.DELTA,
              DataSourceFormat.CSV,
              DataSourceFormat.JSON,
              DataSourceFormat.AVRO,
              DataSourceFormat.PARQUET,
              DataSourceFormat.ORC,
              DataSourceFormat.TEXT,
            ].map((format) => (
              <option key={format} value={format}>
                {DataSourceFormat[format]}
              </option>
            ))}
          </select>
        </FormField>
      )}
      {kind === "volume" && (
        <FormField id="volume-type" label="Volume type">
          <select
            id="volume-type"
            className="h-9 w-full rounded-md border bg-transparent px-3 text-sm"
            value={volumeType}
            onChange={(event) =>
              setVolumeType(Number(event.target.value) as VolumeType)
            }
          >
            <option value={VolumeType.MANAGED}>Managed</option>
            <option value={VolumeType.EXTERNAL}>External</option>
          </select>
        </FormField>
      )}
      {(kind === "table" ||
        (kind === "volume" && volumeType === VolumeType.EXTERNAL)) && (
        <FormField id="object-location" label="Storage location">
          <Input
            id="object-location"
            name="location"
            required
            placeholder="s3://bucket/path"
          />
        </FormField>
      )}
      {(kind === "function" || kind === "view") && (
        <FormField
          id="object-definition"
          label={kind === "view" ? "Metric view YAML" : "Routine definition"}
        >
          <textarea
            id="object-definition"
            name="definition"
            required
            className="min-h-32 w-full rounded-md border bg-transparent px-3 py-2 font-mono text-sm"
          />
        </FormField>
      )}
      {kind === "function" && (
        <FormField id="function-return-type" label="Return type">
          <select
            id="function-return-type"
            name="returnType"
            className="h-9 w-full rounded-md border bg-transparent px-3 text-sm"
            defaultValue={ColumnTypeName.STRING}
          >
            {scalarTypes.map((type) => (
              <option key={type} value={type}>
                {ColumnTypeName[type]}
              </option>
            ))}
          </select>
        </FormField>
      )}
      {kind === "view" && (
        <FormField id="view-dependency" label="Table dependency (full name)">
          <Input
            id="view-dependency"
            name="dependency"
            required
            placeholder="catalog.schema.table"
          />
        </FormField>
      )}
      <FormField id="object-comment" label="Comment">
        <Input id="object-comment" name="comment" />
      </FormField>
    </EntityFormDialog>
  );
}

// ColumnRows edits a column list as repeated `column` and `columnType` form
// fields, which the submit handler reads back in order with FormData.getAll.
// A metric view needs one column per YAML dimension and measure.
function ColumnRows() {
  const [rows, setRows] = useState([0]);
  const nextRow = useRef(1);
  return (
    <fieldset className="space-y-1.5">
      <legend className="text-sm leading-none font-medium">Columns</legend>
      {rows.map((row, index) => (
        <div key={row} className="flex gap-2">
          <Input
            name="column"
            aria-label={`Column ${index + 1} name`}
            required
            defaultValue={row === 0 ? "value" : undefined}
          />
          <select
            name="columnType"
            aria-label={`Column ${index + 1} type`}
            className="h-9 rounded-md border bg-transparent px-3 text-sm"
            defaultValue={ColumnTypeName.STRING}
          >
            {scalarTypes.map((type) => (
              <option key={type} value={type}>
                {ColumnTypeName[type]}
              </option>
            ))}
          </select>
          <Button
            type="button"
            variant="ghost"
            size="icon"
            aria-label={`Remove column ${index + 1}`}
            disabled={rows.length === 1}
            onClick={() => setRows(rows.filter((other) => other !== row))}
          >
            <X className="h-4 w-4" />
          </Button>
        </div>
      ))}
      <Button
        type="button"
        variant="outline"
        size="sm"
        onClick={() => setRows([...rows, nextRow.current++])}
      >
        <Plus className="h-4 w-4" />
        Add column
      </Button>
    </fieldset>
  );
}
