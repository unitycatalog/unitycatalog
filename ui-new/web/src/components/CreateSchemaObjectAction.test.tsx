import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

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

  it("creates an external table", async () => {
    await open("table");
    await userEvent.type(
      screen.getByLabelText("Storage location"),
      "s3://bucket/table",
    );
    await userEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() =>
      expect(mutations.table).toHaveBeenCalledWith(
        expect.objectContaining({
          table: {
            catalogName: "main",
            schemaName: "default",
            name: "table_name",
          },
          storageLocation: { value: "s3://bucket/table" },
        }),
      ),
    );
  });

  it("creates an external volume", async () => {
    await open("volume");
    await userEvent.selectOptions(screen.getByLabelText("Volume type"), "2");
    await userEvent.type(
      screen.getByLabelText("Storage location"),
      "s3://bucket/volume",
    );
    await userEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() =>
      expect(mutations.volume).toHaveBeenCalledWith(
        expect.objectContaining({
          volume: {
            catalogName: "main",
            schemaName: "default",
            name: "volume_name",
          },
          storageLocation: { value: "s3://bucket/volume" },
        }),
      ),
    );
  });

  it.each([
    ["function", "Routine definition", mutations.func],
    ["view", "Metric view YAML", mutations.view],
  ])("creates a %s", async (kind, definitionLabel, mutation) => {
    await open(kind);
    await userEvent.type(screen.getByLabelText(definitionLabel), "SELECT 1");
    if (kind === "view") {
      await userEvent.type(
        screen.getByLabelText("Table dependency (full name)"),
        "main.default.source",
      );
    }
    await userEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => expect(mutation).toHaveBeenCalledOnce());
  });

  it("creates a registered model", async () => {
    await open("model");
    await userEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() =>
      expect(mutations.model).toHaveBeenCalledWith({
        model: {
          catalogName: "main",
          schemaName: "default",
          name: "model_name",
        },
        comment: undefined,
      }),
    );
  });
});
