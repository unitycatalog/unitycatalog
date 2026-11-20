import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ReactElement } from "react";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Code, ConnectError, createRouterTransport } from "@connectrpc/connect";
import { ModelService, ModelVersionStatus } from "@/gen/uc/v1/model_pb";

vi.mock("@tanstack/react-router", () => import("@/test/tanstack-router-mock"));

const { toastError, toastInfo, auth } = vi.hoisted(() => ({
  toastError: vi.fn(),
  toastInfo: vi.fn(),
  auth: {
    current: undefined as
      | { authEnabled: boolean; currentUser: { userName: string } | null }
      | undefined,
  },
}));
vi.mock("@/context/auth-context", () => ({
  useOptionalAuth: () => auth.current,
}));
vi.mock("sonner", async (importOriginal) => {
  const actual = await importOriginal<typeof import("sonner")>();
  return {
    ...actual,
    toast: { ...actual.toast, error: toastError, info: toastInfo },
  };
});

import {
  renderWithProviders,
  type RpcHandlers,
  type UcHandler,
} from "@/test/providers";
import { ColumnTypeName } from "@/gen/uc/v1/common_pb";
import { DataSourceFormat, TableType } from "@/gen/uc/v1/table_pb";
import { VolumeType } from "@/gen/uc/v1/volume_pb";
import CatalogsList from "@/pages/CatalogsList";
import CatalogDetails from "@/pages/CatalogDetails";
import SchemaDetails from "@/pages/SchemaDetails";
import TableDetails from "@/pages/TableDetails";
import VolumeDetails from "@/pages/VolumeDetails";
import FunctionDetails from "@/pages/FunctionDetails";
import ModelDetails from "@/pages/ModelDetails";
import ModelVersionDetails from "@/pages/ModelVersionDetails";
import MetricViewDetails from "@/pages/MetricViewDetails";
import { navigateMock } from "@/test/tanstack-router-mock";

const identity = { catalogName: "main", schemaName: "default" };
const rpc: RpcHandlers = {
  catalogs: {
    listCatalogs: () => ({ catalogs: [{ name: "main", comment: "root" }] }),
    getCatalog: () => ({
      catalog: {
        name: "main",
        comment: "root",
        audit: { owner: "me" },
        properties: { values: { k: "v" } },
      },
    }),
  },
  schemas: {
    listSchemas: () => ({ schemas: [{ ...identity, name: "default" }] }),
    getSchema: () => ({
      schema: { ...identity, name: "default", comment: "sc" },
    }),
  },
  tables: {
    listTables: () => ({
      tables: [{ ...identity, name: "events", tableType: TableType.MANAGED }],
    }),
    getTable: () => ({
      table: {
        ...identity,
        name: "events",
        tableType: TableType.MANAGED,
        dataSourceFormat: DataSourceFormat.DELTA,
        comment: "clickstream",
        columns: [
          {
            name: "id",
            typeText: "string",
            typeName: ColumnTypeName.STRING,
            nullable: true,
            position: 0,
          },
        ],
      },
    }),
  },
  views: {
    listViews: () => ({ views: [{ ...identity, name: "metrics" }] }),
    getView: () => ({
      view: {
        ...identity,
        name: "metrics",
        viewDefinition: "version: 1.1\nsource: main.default.events",
        columns: [
          {
            name: "total",
            typeText: "bigint",
            typeName: ColumnTypeName.LONG,
            nullable: true,
          },
        ],
      },
    }),
  },
  volumes: {
    listVolumes: () => ({ volumes: [{ ...identity, name: "vol" }] }),
    getVolume: () => ({
      volume: {
        ...identity,
        name: "vol",
        volumeType: VolumeType.MANAGED,
        storageLocation: "s3://x",
      },
    }),
  },
  functions: {
    listFunctions: () => ({ functions: [{ ...identity, name: "fn" }] }),
    getFunction: () => ({
      function: {
        ...identity,
        name: "fn",
        inputParameters: [
          {
            name: "x",
            typeText: "int",
            typeName: ColumnTypeName.INT,
            position: 0,
          },
        ],
      },
    }),
  },
  models: {
    listRegisteredModels: () => ({ models: [{ ...identity, name: "mdl" }] }),
    getRegisteredModel: () => ({
      model: { ...identity, name: "mdl", comment: "a model" },
    }),
    listModelVersions: () => ({
      versions: [
        {
          ...identity,
          modelName: "mdl",
          version: 1n,
          status: ModelVersionStatus.READY,
        },
      ],
    }),
    getModelVersion: () => ({
      modelVersion: {
        ...identity,
        modelName: "mdl",
        version: 1n,
        status: ModelVersionStatus.READY,
        source: "s3://m",
      },
    }),
  },
};

const handler: UcHandler = ({ path }) => ({
  httpStatus: 200,
  ok: true,
  body: JSON.stringify(
    path.includes("/permissions/")
      ? {
          privilege_assignments: [{ principal: "ada", privileges: ["SELECT"] }],
        }
      : {},
  ),
});

const options = { handler, rpc };

async function clickTabs() {
  // Tabs live inside QueryState, so wait for them to mount once data resolves.
  await userEvent.click(await screen.findByRole("tab", { name: "Details" }));
  await userEvent.click(
    await screen.findByRole("tab", { name: "Permissions" }),
  );
}

describe("CatalogsList", () => {
  it("lists catalogs", async () => {
    renderWithProviders(<CatalogsList />, options);
    expect(await screen.findByText("main")).toBeInTheDocument();
    expect(
      screen.getByRole("heading", { name: "Catalogs" }),
    ).toBeInTheDocument();
  });

  it("shows an empty state", async () => {
    renderWithProviders(<CatalogsList />, {
      rpc: { catalogs: { listCatalogs: () => ({ catalogs: [] }) } },
    });
    expect(await screen.findByText("No catalogs yet.")).toBeInTheDocument();
  });
});

describe("CatalogDetails", () => {
  it("renders overview, details, and permissions", async () => {
    renderWithProviders(<CatalogDetails catalog="main" />, options);
    expect(
      await screen.findByRole("heading", { name: "main" }),
    ).toBeInTheDocument();
    expect(await screen.findByText("root")).toBeInTheDocument();
    expect(await screen.findByText("default")).toBeInTheDocument();
    await clickTabs();
    await waitFor(() => expect(screen.getByText("ada")).toBeInTheDocument());
  });
});

describe("SchemaDetails", () => {
  it("renders the object browser and tabs", async () => {
    renderWithProviders(<SchemaDetails catalog="main" schema="default" />, {
      ...options,
    });
    expect(
      await screen.findByRole("heading", { name: "default" }),
    ).toBeInTheDocument();
    expect(await screen.findByText("events")).toBeInTheDocument();
    expect(await screen.findByText("vol")).toBeInTheDocument();
    expect(await screen.findByText("fn")).toBeInTheDocument();
    expect(await screen.findByText("mdl")).toBeInTheDocument();
    await clickTabs();
    await waitFor(() => expect(screen.getByText("ada")).toBeInTheDocument());
  });
});

describe("TableDetails", () => {
  it("renders columns and metadata", async () => {
    renderWithProviders(
      <TableDetails catalog="main" schema="default" table="events" />,
      options,
    );
    expect(
      await screen.findByRole("heading", { name: "events" }),
    ).toBeInTheDocument();
    // Badges + columns come from the async table fetch.
    expect(await screen.findByText("MANAGED")).toBeInTheDocument();
    expect(screen.getByText("clickstream")).toBeInTheDocument();
    expect(screen.getByText("id")).toBeInTheDocument();
    await clickTabs();
    await waitFor(() => expect(screen.getByText("ada")).toBeInTheDocument());
  });
});

describe("MetricViewDetails", () => {
  it("renders columns, definition, and metadata", async () => {
    renderWithProviders(
      <MetricViewDetails catalog="main" schema="default" view="metrics" />,
      options,
    );
    expect(
      await screen.findByRole("heading", { name: "metrics" }),
    ).toBeInTheDocument();
    expect(await screen.findByText("total")).toBeInTheDocument();
    expect(screen.getByText(/source: main.default.events/)).toBeInTheDocument();
  });
});

// Tables and metric views share one UC resource, so each page's delete must
// refuse the other kind rather than remove it.
describe("delete on a table or metric view page", () => {
  beforeEach(() => {
    toastError.mockClear();
    navigateMock.mockClear();
  });

  const refuse = (message: string, code: Code) => () => {
    throw new ConnectError(message, code);
  };
  const tableIsNotAView = "main.default.events is a table, not a metric view";
  const viewIsNotATable = "main.default.events is a metric view, not a table";

  it.each([
    {
      kind: "metric view",
      page: <MetricViewDetails catalog="main" schema="default" view="events" />,
      mismatch: tableIsNotAView,
      rpc: {
        ...rpc,
        views: {
          getView: refuse(tableIsNotAView, Code.NotFound),
          deleteView: refuse(
            `${tableIsNotAView}; nothing was deleted`,
            Code.FailedPrecondition,
          ),
        },
      },
    },
    {
      kind: "table",
      page: <TableDetails catalog="main" schema="default" table="events" />,
      mismatch: viewIsNotATable,
      rpc: {
        ...rpc,
        tables: {
          getTable: refuse(viewIsNotATable, Code.NotFound),
          deleteTable: refuse(
            `${viewIsNotATable}; nothing was deleted`,
            Code.FailedPrecondition,
          ),
        },
      },
    },
  ])(
    "keeps the $kind delete available but reports a refused other kind",
    async ({ kind, page, mismatch, rpc }) => {
      renderWithProviders(page, { handler, rpc });
      expect(
        await screen.findByText(`[not_found] ${mismatch}`),
      ).toBeInTheDocument();

      await userEvent.click(
        screen.getByRole("button", { name: `Delete ${kind}` }),
      );
      expect(
        screen.getByRole("dialog", { name: `Delete ${kind} events?` }),
      ).toBeInTheDocument();
      await userEvent.type(
        screen.getByLabelText("Enter events to confirm"),
        "events",
      );
      await userEvent.click(
        screen.getByRole("button", { name: "Delete permanently" }),
      );

      await waitFor(() =>
        expect(toastError).toHaveBeenCalledWith(
          `[failed_precondition] ${mismatch}; nothing was deleted`,
        ),
      );
      expect(screen.getByRole("dialog")).toBeInTheDocument();
      expect(navigateMock).not.toHaveBeenCalled();
    },
  );
});

describe("VolumeDetails", () => {
  it("renders overview + details", async () => {
    renderWithProviders(
      <VolumeDetails catalog="main" schema="default" volume="vol" />,
      options,
    );
    expect(
      await screen.findByRole("heading", { name: "vol" }),
    ).toBeInTheDocument();
    // Storage location lives on the Details tab.
    await userEvent.click(await screen.findByRole("tab", { name: "Details" }));
    await waitFor(() => expect(screen.getByText("s3://x")).toBeInTheDocument());
    await userEvent.click(screen.getByRole("tab", { name: "Permissions" }));
    await waitFor(() => expect(screen.getByText("ada")).toBeInTheDocument());
  });
});

describe("FunctionDetails", () => {
  it("renders input parameters", async () => {
    renderWithProviders(
      <FunctionDetails catalog="main" schema="default" ucFunction="fn" />,
      options,
    );
    expect(
      await screen.findByRole("heading", { name: "fn" }),
    ).toBeInTheDocument();
    expect(await screen.findByText("x")).toBeInTheDocument();
    await clickTabs();
    await waitFor(() => expect(screen.getByText("ada")).toBeInTheDocument());
  });
});

describe("ModelDetails", () => {
  it("lists versions and links to a version", async () => {
    renderWithProviders(
      <ModelDetails catalog="main" schema="default" model="mdl" />,
      options,
    );
    expect(
      await screen.findByRole("heading", { name: "mdl" }),
    ).toBeInTheDocument();
    expect(await screen.findByRole("link", { name: "v1" })).toHaveAttribute(
      "href",
      "/catalog/main/default/model/mdl/version/1",
    );
    await clickTabs();
    await waitFor(() => expect(screen.getByText("ada")).toBeInTheDocument());
  });
});

describe("object lists", () => {
  const fail = () => {
    throw new ConnectError("list failed", Code.Unavailable);
  };
  it.each([
    {
      page: "SchemaDetails",
      ui: <SchemaDetails catalog="main" schema="default" />,
      failing: { tables: { ...rpc.tables, listTables: fail } },
    },
    {
      page: "CatalogDetails",
      ui: <CatalogDetails catalog="main" />,
      failing: { schemas: { ...rpc.schemas, listSchemas: fail } },
    },
    {
      page: "ModelDetails",
      ui: <ModelDetails catalog="main" schema="default" model="mdl" />,
      failing: { models: { ...rpc.models, listModelVersions: fail } },
    },
  ] satisfies { page: string; ui: ReactElement; failing: RpcHandlers }[])(
    "$page shows a failed list instead of an empty one",
    async (list) => {
      renderWithProviders(list.ui, {
        handler,
        rpc: { ...rpc, ...list.failing },
      });

      expect(
        await screen.findByText("[unavailable] list failed"),
      ).toBeInTheDocument();
    },
  );
});

describe("ModelVersionDetails", () => {
  afterEach(() => {
    auth.current = undefined;
    toastError.mockClear();
    toastInfo.mockClear();
  });

  it("renders version metadata", async () => {
    renderWithProviders(
      <ModelVersionDetails
        catalog="main"
        schema="default"
        model="mdl"
        version="1"
      />,
      options,
    );
    expect(await screen.findByText("s3://m")).toBeInTheDocument();
  });

  // Renders a pending version created by ada@example.com whose finalize fails.
  function renderPendingVersion(comment = "") {
    const updateModelVersion = vi.fn(() => ({}));
    const transport = createRouterTransport(({ service }) => {
      service(ModelService, {
        getModelVersion: () => ({
          modelVersion: {
            version: 1n,
            comment,
            status: ModelVersionStatus.PENDING_REGISTRATION,
            audit: { createdBy: "ada@example.com" },
          },
        }),
        updateModelVersion,
        finalizeModelVersion: () => {
          throw new ConnectError(
            "only the creator can finalize",
            Code.PermissionDenied,
          );
        },
      });
    });
    renderWithProviders(
      <ModelVersionDetails
        catalog="main"
        schema="default"
        model="mdl"
        version="1"
      />,
      { transport },
    );
    return updateModelVersion;
  }

  it.each([
    { when: "auth is disabled", authEnabled: false, user: null, shown: true },
    {
      when: "the creator is signed in",
      authEnabled: true,
      user: "ada@example.com",
      shown: true,
    },
    {
      when: "another user is signed in",
      authEnabled: true,
      user: "bob@example.com",
      shown: false,
    },
  ])("offers Finalize only to the creator: $when", async (scenario) => {
    auth.current = {
      authEnabled: scenario.authEnabled,
      currentUser: scenario.user ? { userName: scenario.user } : null,
    };
    renderPendingVersion();

    await screen.findAllByText("PENDING_REGISTRATION");
    expect(screen.queryByRole("button", { name: "Finalize" }) !== null).toBe(
      scenario.shown,
    );
  });

  it.each(["", "existing comment"])(
    "does not submit an unchanged model-version comment (%s)",
    async (comment) => {
      const updateModelVersion = renderPendingVersion(comment);
      await screen.findAllByText("PENDING_REGISTRATION");
      await userEvent.click(
        await screen.findByRole("button", { name: "Edit comment" }),
      );
      await userEvent.click(screen.getByRole("button", { name: "Save" }));

      expect(updateModelVersion).not.toHaveBeenCalled();
      expect(toastInfo).toHaveBeenCalledWith("No changes to save.");
      expect(screen.getByRole("dialog")).toBeInTheDocument();
    },
  );

  it.each(["edited comment", ""])(
    "submits a changed model-version comment (%s)",
    async (comment) => {
      const updateModelVersion = renderPendingVersion("existing comment");
      await screen.findAllByText("PENDING_REGISTRATION");
      await userEvent.click(
        screen.getByRole("button", { name: "Edit comment" }),
      );
      const input = screen.getByRole("textbox", { name: "Comment" });
      await userEvent.clear(input);
      if (comment) await userEvent.type(input, comment);
      await userEvent.click(screen.getByRole("button", { name: "Save" }));

      await waitFor(() => expect(updateModelVersion).toHaveBeenCalledOnce());
      expect(updateModelVersion).toHaveBeenCalledWith(
        expect.objectContaining({
          comment,
          modelVersion: expect.objectContaining({ version: 1n }),
        }),
        expect.anything(),
      );
      await waitFor(() =>
        expect(screen.queryByRole("dialog")).not.toBeInTheDocument(),
      );
    },
  );

  it("reports a failed finalize and re-enables the button", async () => {
    renderPendingVersion();

    const finalize = await screen.findByRole("button", { name: "Finalize" });
    await userEvent.click(finalize);

    await waitFor(() =>
      expect(toastError).toHaveBeenCalledWith(
        "Finalize failed: [permission_denied] only the creator can finalize",
      ),
    );
    expect(finalize).toBeEnabled();
  });

  it("rejects invalid version route parameters without crashing", async () => {
    renderWithProviders(
      <ModelVersionDetails
        catalog="main"
        schema="default"
        model="mdl"
        version="invalid"
      />,
      options,
    );

    expect(
      await screen.findByText("Invalid model version: invalid"),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Edit comment" }),
    ).not.toBeInTheDocument();
  });
});
