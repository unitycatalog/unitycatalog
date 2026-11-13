import { afterEach, describe, expect, it, vi } from "vitest";
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

import { renderWithProviders, type UcHandler } from "@/test/providers";
import CatalogsList from "@/pages/CatalogsList";
import CatalogDetails from "@/pages/CatalogDetails";
import SchemaDetails from "@/pages/SchemaDetails";
import TableDetails from "@/pages/TableDetails";
import VolumeDetails from "@/pages/VolumeDetails";
import FunctionDetails from "@/pages/FunctionDetails";
import ModelDetails from "@/pages/ModelDetails";
import ModelVersionDetails from "@/pages/ModelVersionDetails";
import MetricViewDetails from "@/pages/MetricViewDetails";

// A broad handler answering every read the detail pages make.
const handler: UcHandler = ({ path }) => {
  const body = (o: unknown) => ({
    httpStatus: 200,
    ok: true,
    body: JSON.stringify(o),
  });
  switch (true) {
    case path === "/api/2.1/unity-catalog/catalogs":
      return body({
        catalogs: [
          { name: "main", comment: "root", updated_at: 1_700_000_000_000 },
        ],
      });
    case path === "/api/2.1/unity-catalog/catalogs/main":
      return body({
        name: "main",
        comment: "root",
        owner: "me",
        properties: { k: "v" },
      });
    case path === "/api/2.1/unity-catalog/schemas":
      return body({ schemas: [{ name: "default" }] });
    case path === "/api/2.1/unity-catalog/schemas/main.default":
      return body({ name: "default", catalog_name: "main", comment: "sc" });
    case path === "/api/2.1/unity-catalog/tables":
      return body({
        tables: [
          { name: "events", table_type: "MANAGED" },
          { name: "metrics", table_type: "METRIC_VIEW" },
        ],
      });
    case path === "/api/2.1/unity-catalog/tables/main.default.events":
      return body({
        name: "events",
        table_type: "MANAGED",
        data_source_format: "DELTA",
        comment: "clickstream",
        columns: [
          { name: "id", type_text: "string", nullable: true, position: 0 },
        ],
      });
    case path === "/api/2.1/unity-catalog/tables/main.default.metrics":
      return body({
        name: "metrics",
        table_type: "METRIC_VIEW",
        view_definition: "version: 1.1\nsource: main.default.events",
        columns: [{ name: "total", type_text: "bigint", nullable: true }],
      });
    case path === "/api/2.1/unity-catalog/volumes":
      return body({ volumes: [{ name: "vol" }] });
    case path === "/api/2.1/unity-catalog/volumes/main.default.vol":
      return body({
        name: "vol",
        volume_type: "MANAGED",
        storage_location: "s3://x",
      });
    case path === "/api/2.1/unity-catalog/functions":
      return body({ functions: [{ name: "fn" }] });
    case path === "/api/2.1/unity-catalog/functions/main.default.fn":
      return body({
        name: "fn",
        input_params: {
          parameters: [{ name: "x", type_text: "int", position: 0 }],
        },
      });
    case path === "/api/2.1/unity-catalog/models":
      return body({ registered_models: [{ name: "mdl" }] });
    case path === "/api/2.1/unity-catalog/models/main.default.mdl":
      return body({ name: "mdl", comment: "a model" });
    case path === "/api/2.1/unity-catalog/models/main.default.mdl/versions":
      return body({
        model_versions: [
          { version: 1, status: "READY", created_at: 1_700_000_000_000 },
        ],
      });
    case path === "/api/2.1/unity-catalog/models/main.default.mdl/versions/1":
      return body({
        model_name: "mdl",
        version: 1,
        status: "READY",
        source: "s3://m",
      });
    case path.includes("/permissions/"):
      return body({
        privilege_assignments: [{ principal: "ada", privileges: ["SELECT"] }],
      });
    default:
      return { httpStatus: 404, ok: false, body: "{}" };
  }
};

async function clickTabs() {
  // Tabs live inside QueryState, so wait for them to mount once data resolves.
  await userEvent.click(await screen.findByRole("tab", { name: "Details" }));
  await userEvent.click(
    await screen.findByRole("tab", { name: "Permissions" }),
  );
}

describe("CatalogsList", () => {
  it("lists catalogs", async () => {
    renderWithProviders(<CatalogsList />, { handler });
    expect(await screen.findByText("main")).toBeInTheDocument();
    expect(
      screen.getByRole("heading", { name: "Catalogs" }),
    ).toBeInTheDocument();
  });

  it("shows an empty state", async () => {
    renderWithProviders(<CatalogsList />, {
      handler: () => ({
        httpStatus: 200,
        ok: true,
        body: JSON.stringify({ catalogs: [] }),
      }),
    });
    expect(await screen.findByText("No catalogs yet.")).toBeInTheDocument();
  });
});

describe("CatalogDetails", () => {
  it("renders overview, details, and permissions", async () => {
    renderWithProviders(<CatalogDetails catalog="main" />, { handler });
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
      handler,
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
      { handler },
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
      { handler },
    );
    expect(
      await screen.findByRole("heading", { name: "metrics" }),
    ).toBeInTheDocument();
    expect(await screen.findByText("total")).toBeInTheDocument();
    expect(screen.getByText(/source: main.default.events/)).toBeInTheDocument();
  });
});

describe("VolumeDetails", () => {
  it("renders overview + details", async () => {
    renderWithProviders(
      <VolumeDetails catalog="main" schema="default" volume="vol" />,
      { handler },
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
      { handler },
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
      { handler },
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
  it.each([
    {
      page: "SchemaDetails",
      ui: <SchemaDetails catalog="main" schema="default" />,
      path: "/api/2.1/unity-catalog/tables",
    },
    {
      page: "CatalogDetails",
      ui: <CatalogDetails catalog="main" />,
      path: "/api/2.1/unity-catalog/schemas",
    },
    {
      page: "ModelDetails",
      ui: <ModelDetails catalog="main" schema="default" model="mdl" />,
      path: "/api/2.1/unity-catalog/models/main.default.mdl/versions",
    },
  ])("$page shows a failed list instead of an empty one", async (list) => {
    const failing: UcHandler = (call) =>
      call.path === list.path
        ? { httpStatus: 500, ok: false, body: "" }
        : handler(call);
    renderWithProviders(list.ui, { handler: failing });

    // The in-memory transport reports the handler's failure as an internal error.
    const errors = await screen.findAllByText("[internal] internal error");
    expect(errors.length).toBeGreaterThan(0);
  });
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
      { handler },
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
      { handler },
    );

    expect(
      await screen.findByText("Invalid model version: invalid"),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Edit comment" }),
    ).not.toBeInTheDocument();
  });
});
