import { describe, expect, it, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

vi.mock("@tanstack/react-router", () => import("@/test/tanstack-router-mock"));

import { renderWithProviders, type UcHandler } from "@/test/providers";
import CatalogsList from "@/pages/CatalogsList";
import CatalogDetails from "@/pages/CatalogDetails";
import SchemaDetails from "@/pages/SchemaDetails";
import TableDetails from "@/pages/TableDetails";
import VolumeDetails from "@/pages/VolumeDetails";
import FunctionDetails from "@/pages/FunctionDetails";
import ModelDetails from "@/pages/ModelDetails";
import ModelVersionDetails from "@/pages/ModelVersionDetails";

// A broad handler answering every read the detail pages make.
const handler: UcHandler = ({ path }) => {
  const body = (o: unknown) => ({ httpStatus: 200, ok: true, body: JSON.stringify(o) });
  switch (true) {
    case path === "/api/2.1/unity-catalog/catalogs":
      return body({ catalogs: [{ name: "main", comment: "root", updated_at: 1_700_000_000_000 }] });
    case path === "/api/2.1/unity-catalog/catalogs/main":
      return body({ name: "main", comment: "root", owner: "me", properties: { k: "v" } });
    case path === "/api/2.1/unity-catalog/schemas":
      return body({ schemas: [{ name: "default" }] });
    case path === "/api/2.1/unity-catalog/schemas/main.default":
      return body({ name: "default", catalog_name: "main", comment: "sc" });
    case path === "/api/2.1/unity-catalog/tables":
      return body({ tables: [{ name: "events" }] });
    case path === "/api/2.1/unity-catalog/tables/main.default.events":
      return body({
        name: "events",
        table_type: "MANAGED",
        data_source_format: "DELTA",
        comment: "clickstream",
        columns: [{ name: "id", type_text: "string", nullable: true, position: 0 }],
      });
    case path === "/api/2.1/unity-catalog/volumes":
      return body({ volumes: [{ name: "vol" }] });
    case path === "/api/2.1/unity-catalog/volumes/main.default.vol":
      return body({ name: "vol", volume_type: "MANAGED", storage_location: "s3://x" });
    case path === "/api/2.1/unity-catalog/functions":
      return body({ functions: [{ name: "fn" }] });
    case path === "/api/2.1/unity-catalog/functions/main.default.fn":
      return body({ name: "fn", input_params: { parameters: [{ name: "x", type_text: "int", position: 0 }] } });
    case path === "/api/2.1/unity-catalog/models":
      return body({ registered_models: [{ name: "mdl" }] });
    case path === "/api/2.1/unity-catalog/models/main.default.mdl":
      return body({ name: "mdl", comment: "a model" });
    case path === "/api/2.1/unity-catalog/models/main.default.mdl/versions":
      return body({ model_versions: [{ version: 1, status: "READY", created_at: 1_700_000_000_000 }] });
    case path === "/api/2.1/unity-catalog/models/main.default.mdl/versions/1":
      return body({ model_name: "mdl", version: 1, status: "READY", source: "s3://m" });
    case path.includes("/permissions/"):
      return body({ privilege_assignments: [{ principal: "ada", privileges: ["SELECT"] }] });
    default:
      return { httpStatus: 404, ok: false, body: "{}" };
  }
};

async function clickTabs() {
  // Tabs live inside QueryState, so wait for them to mount once data resolves.
  await userEvent.click(await screen.findByRole("tab", { name: "Details" }));
  await userEvent.click(await screen.findByRole("tab", { name: "Permissions" }));
}

describe("CatalogsList", () => {
  it("lists catalogs", async () => {
    renderWithProviders(<CatalogsList />, { handler });
    expect(await screen.findByText("main")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Catalogs" })).toBeInTheDocument();
  });

  it("shows an empty state", async () => {
    renderWithProviders(<CatalogsList />, {
      handler: () => ({ httpStatus: 200, ok: true, body: JSON.stringify({ catalogs: [] }) }),
    });
    expect(await screen.findByText("No catalogs yet.")).toBeInTheDocument();
  });
});

describe("CatalogDetails", () => {
  it("renders overview, details, and permissions", async () => {
    renderWithProviders(<CatalogDetails catalog="main" />, { handler });
    expect(await screen.findByRole("heading", { name: "main" })).toBeInTheDocument();
    expect(await screen.findByText("root")).toBeInTheDocument();
    expect(await screen.findByText("default")).toBeInTheDocument();
    await clickTabs();
    await waitFor(() => expect(screen.getByText("ada")).toBeInTheDocument());
  });
});

describe("SchemaDetails", () => {
  it("renders the object browser and tabs", async () => {
    renderWithProviders(<SchemaDetails catalog="main" schema="default" />, { handler });
    expect(await screen.findByRole("heading", { name: "default" })).toBeInTheDocument();
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
    renderWithProviders(<TableDetails catalog="main" schema="default" table="events" />, { handler });
    expect(await screen.findByRole("heading", { name: "events" })).toBeInTheDocument();
    // Badges + columns come from the async table fetch.
    expect(await screen.findByText("MANAGED")).toBeInTheDocument();
    expect(screen.getByText("clickstream")).toBeInTheDocument();
    expect(screen.getByText("id")).toBeInTheDocument();
    await clickTabs();
    await waitFor(() => expect(screen.getByText("ada")).toBeInTheDocument());
  });
});

describe("VolumeDetails", () => {
  it("renders overview + details", async () => {
    renderWithProviders(<VolumeDetails catalog="main" schema="default" volume="vol" />, { handler });
    expect(await screen.findByRole("heading", { name: "vol" })).toBeInTheDocument();
    // Storage location lives on the Details tab.
    await userEvent.click(await screen.findByRole("tab", { name: "Details" }));
    await waitFor(() => expect(screen.getByText("s3://x")).toBeInTheDocument());
    await userEvent.click(screen.getByRole("tab", { name: "Permissions" }));
    await waitFor(() => expect(screen.getByText("ada")).toBeInTheDocument());
  });
});

describe("FunctionDetails", () => {
  it("renders input parameters", async () => {
    renderWithProviders(<FunctionDetails catalog="main" schema="default" ucFunction="fn" />, { handler });
    expect(await screen.findByRole("heading", { name: "fn" })).toBeInTheDocument();
    expect(await screen.findByText("x")).toBeInTheDocument();
    await clickTabs();
    await waitFor(() => expect(screen.getByText("ada")).toBeInTheDocument());
  });
});

describe("ModelDetails", () => {
  it("lists versions and links to a version", async () => {
    renderWithProviders(<ModelDetails catalog="main" schema="default" model="mdl" />, { handler });
    expect(await screen.findByRole("heading", { name: "mdl" })).toBeInTheDocument();
    expect(await screen.findByRole("link", { name: "v1" })).toHaveAttribute(
      "href",
      "/catalog/main/default/model/mdl/version/1",
    );
    await clickTabs();
    await waitFor(() => expect(screen.getByText("ada")).toBeInTheDocument());
  });
});

describe("ModelVersionDetails", () => {
  it("renders version metadata", async () => {
    renderWithProviders(
      <ModelVersionDetails catalog="main" schema="default" model="mdl" version="1" />,
      { handler },
    );
    expect(await screen.findByText("s3://m")).toBeInTheDocument();
  });
});
