import { beforeEach, describe, expect, it, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createRouterTransport, type Transport } from "@connectrpc/connect";
import { ColumnTypeName } from "@/gen/uc/v1/common_pb";
import { DataSourceFormat } from "@/gen/uc/v1/table_pb";
import { ViewService, type CreateViewRequest } from "@/gen/uc/v1/view_pb";
import { VolumeType } from "@/gen/uc/v1/volume_pb";
import { renderWithProviders } from "@/test/providers";

const mutations = vi.hoisted(() => ({
  table: vi.fn(),
  volume: vi.fn(),
  func: vi.fn(),
  model: vi.fn(),
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

import CreateSchemaObjectAction from "@/components/CreateSchemaObjectAction";

describe("CreateSchemaObjectAction", () => {
  beforeEach(() => {
    for (const mutation of Object.values(mutations)) {
      mutation.mockReset().mockResolvedValue(undefined);
    }
  });

  async function open(kind: string, transport?: Transport) {
    renderWithProviders(
      <CreateSchemaObjectAction catalog="main" schema="default" />,
      { transport },
    );
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
      screen.getByLabelText("Column 1 type"),
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

  it("sends a column for every metric view dimension and measure", async () => {
    const definition = [
      "version: 0.1",
      "source: main.default.orders",
      "dimensions:",
      "  - name: region",
      "    expr: region",
      "measures:",
      "  - name: revenue",
      "    expr: SUM(amount)",
    ].join("\n");
    let request: CreateViewRequest | undefined;
    const transport = createRouterTransport(({ service }) => {
      service(ViewService, {
        createView(input) {
          request = input;
          return {};
        },
      });
    });
    await open("view", transport);
    expect(
      screen.getByRole("button", { name: "Remove column 1" }),
    ).toBeDisabled();

    await userEvent.click(screen.getByLabelText("Metric view YAML"));
    await userEvent.paste(definition);
    await userEvent.click(screen.getByRole("button", { name: "Add column" }));
    await userEvent.click(screen.getByRole("button", { name: "Add column" }));
    await userEvent.click(
      screen.getByRole("button", { name: "Remove column 2" }),
    );
    const firstName = screen.getByLabelText("Column 1 name");
    await userEvent.clear(firstName);
    await userEvent.type(firstName, "region");
    await userEvent.type(screen.getByLabelText("Column 2 name"), "revenue");
    await userEvent.selectOptions(
      screen.getByLabelText("Column 2 type"),
      String(ColumnTypeName.DOUBLE),
    );
    await userEvent.type(
      screen.getByLabelText("Table dependency (full name)"),
      "main.default.orders",
    );
    await userEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => expect(request).toBeDefined());
    expect(request).toMatchObject({
      view: reference("view"),
      columns: [
        { name: "region", typeName: ColumnTypeName.STRING, nullable: true },
        { name: "revenue", typeName: ColumnTypeName.DOUBLE, nullable: true },
      ],
      viewDefinition: definition,
      dependencies: [
        { target: { case: "tableFullName", value: "main.default.orders" } },
      ],
    });
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
