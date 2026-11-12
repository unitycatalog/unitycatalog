import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ColumnTypeName } from "@/gen/uc/v1/common_pb";
import { DataSourceFormat } from "@/gen/uc/v1/table_pb";
import { VolumeType } from "@/gen/uc/v1/volume_pb";

const mutations = vi.hoisted(() => ({
  table: vi.fn(),
  volume: vi.fn(),
  func: vi.fn(),
  model: vi.fn(),
  view: vi.fn(),
}));

vi.mock("@/hooks/tables", () => ({
  useCreateTable: () => ({ mutateAsync: mutations.table }),
}));
vi.mock("@/hooks/volumes", () => ({
  useCreateVolume: () => ({ mutateAsync: mutations.volume }),
}));
vi.mock("@/hooks/functions", () => ({
  useCreateFunction: () => ({ mutateAsync: mutations.func }),
}));
vi.mock("@/hooks/models", () => ({
  useCreateModel: () => ({ mutateAsync: mutations.model }),
}));
vi.mock("@/hooks/views", () => ({
  useCreateView: () => ({ mutateAsync: mutations.view }),
}));

import CreateSchemaObjectAction from "@/components/CreateSchemaObjectAction";

describe("CreateSchemaObjectAction", () => {
  beforeEach(() => {
    for (const mutation of Object.values(mutations)) {
      mutation.mockReset().mockResolvedValue(undefined);
    }
  });

  async function open(kind: string) {
    render(<CreateSchemaObjectAction catalog="main" schema="default" />);
    await userEvent.click(
      screen.getByRole("button", { name: "Create object" }),
    );
    await userEvent.selectOptions(screen.getByLabelText("Object type"), kind);
    await userEvent.type(screen.getByLabelText("Name"), `${kind}_name`);
  }

  const reference = (kind: string) => ({
    catalogName: "main",
    schemaName: "default",
    name: `${kind}_name`,
  });

  it("creates an external table from every table field", async () => {
    await open("table");
    await userEvent.selectOptions(
      screen.getByLabelText("Data source format"),
      String(DataSourceFormat.PARQUET),
    );
    await userEvent.selectOptions(
      screen.getByLabelText("Column type"),
      String(ColumnTypeName.INT),
    );
    await userEvent.type(
      screen.getByLabelText("Storage location"),
      "s3://bucket/table",
    );
    await userEvent.type(screen.getByLabelText("Comment"), "landing table");
    await userEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() =>
      expect(mutations.table).toHaveBeenCalledWith({
        table: reference("table"),
        dataSourceFormat: DataSourceFormat.PARQUET,
        columns: [
          { name: "value", typeName: ColumnTypeName.INT, nullable: true },
        ],
        storageLocation: { value: "s3://bucket/table" },
        comment: "landing table",
      }),
    );
  });

  it("creates an external volume", async () => {
    await open("volume");
    await userEvent.selectOptions(
      screen.getByLabelText("Volume type"),
      String(VolumeType.EXTERNAL),
    );
    await userEvent.type(
      screen.getByLabelText("Storage location"),
      "s3://bucket/volume",
    );
    await userEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() =>
      expect(mutations.volume).toHaveBeenCalledWith({
        volume: reference("volume"),
        volumeType: VolumeType.EXTERNAL,
        storageLocation: { value: "s3://bucket/volume" },
        comment: undefined,
      }),
    );
  });

  it("creates a function with its return type and definition", async () => {
    await open("function");
    await userEvent.selectOptions(
      screen.getByLabelText("Return type"),
      String(ColumnTypeName.LONG),
    );
    await userEvent.type(
      screen.getByLabelText("Routine definition"),
      "RETURN 42",
    );
    await userEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() =>
      expect(mutations.func).toHaveBeenCalledWith({
        function: reference("function"),
        returnType: ColumnTypeName.LONG,
        routineDefinition: "RETURN 42",
        comment: undefined,
      }),
    );
  });

  it("creates a metric view with its column and table dependency", async () => {
    await open("view");
    await userEvent.type(
      screen.getByLabelText("Metric view YAML"),
      "version: 1.1",
    );
    await userEvent.type(
      screen.getByLabelText("Table dependency (full name)"),
      "main.default.source",
    );
    await userEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() =>
      expect(mutations.view).toHaveBeenCalledWith({
        view: reference("view"),
        columns: [
          { name: "value", typeName: ColumnTypeName.STRING, nullable: true },
        ],
        viewDefinition: "version: 1.1",
        dependencies: [
          { target: { case: "tableFullName", value: "main.default.source" } },
        ],
        comment: undefined,
      }),
    );
  });

  it("creates a registered model", async () => {
    await open("model");
    await userEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() =>
      expect(mutations.model).toHaveBeenCalledWith({
        model: reference("model"),
        comment: undefined,
      }),
    );
  });
});
