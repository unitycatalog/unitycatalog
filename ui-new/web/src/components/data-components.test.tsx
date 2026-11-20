import { describe, expect, it, vi, beforeEach } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

vi.mock("@tanstack/react-router", () => import("@/test/tanstack-router-mock"));

import { setParams } from "@/test/tanstack-router-mock";
import {
  renderWithProviders,
  type RpcHandlers,
  type UcHandler,
} from "@/test/providers";
import { TableType } from "@/gen/uc/v1/table_pb";
import PermissionsPanel from "@/components/PermissionsPanel";
import CatalogTree from "@/components/CatalogTree";

beforeEach(() => setParams({}));

describe("PermissionsPanel", () => {
  const handler: UcHandler = ({ path }) =>
    path.includes("/permissions/")
      ? {
          httpStatus: 200,
          ok: true,
          body: JSON.stringify({
            privilege_assignments: [
              { principal: "ada@x.io", privileges: ["SELECT", "MODIFY"] },
            ],
          }),
        }
      : { httpStatus: 404, ok: false, body: "{}" };

  it("renders principals and their privileges", async () => {
    renderWithProviders(
      <PermissionsPanel securableType="table" fullName="main.default.events" />,
      { handler },
    );
    await waitFor(() =>
      expect(screen.getByText("ada@x.io")).toBeInTheDocument(),
    );
    expect(screen.getByText("SELECT")).toBeInTheDocument();
    expect(screen.getByText("MODIFY")).toBeInTheDocument();
  });

  it("shows a placeholder when there are no grants", async () => {
    renderWithProviders(
      <PermissionsPanel securableType="catalog" fullName="main" />,
      {
        handler: () => ({
          httpStatus: 200,
          ok: true,
          body: JSON.stringify({ privilege_assignments: [] }),
        }),
      },
    );
    await waitFor(() =>
      expect(screen.getByText("No privileges granted.")).toBeInTheDocument(),
    );
  });
});

describe("CatalogTree", () => {
  const identity = { catalogName: "main", schemaName: "default" };
  const rpc: RpcHandlers = {
    catalogs: { listCatalogs: () => ({ catalogs: [{ name: "main" }] }) },
    schemas: {
      listSchemas: () => ({ schemas: [{ ...identity, name: "default" }] }),
    },
    tables: {
      listTables: () => ({
        tables: [{ ...identity, name: "t1", tableType: TableType.MANAGED }],
      }),
    },
    views: {
      listViews: () => ({ views: [{ ...identity, name: "mv1" }] }),
    },
    volumes: {
      listVolumes: () => ({ volumes: [{ ...identity, name: "v1" }] }),
    },
    functions: {
      listFunctions: () => ({ functions: [{ ...identity, name: "f1" }] }),
    },
    models: {
      listRegisteredModels: () => ({
        models: [{ ...identity, name: "m1" }],
      }),
    },
  };

  it("auto-expands the active catalog/schema and lazy-loads each group", async () => {
    // Seed the route so the catalog + schema nodes open on mount and Tables
    // (defaultOpen) loads without a click.
    setParams({ catalog: "main", schema: "default" });
    renderWithProviders(<CatalogTree />, { rpc });

    expect(await screen.findByText("main")).toBeInTheDocument();
    expect(await screen.findByText("default")).toBeInTheDocument();
    expect(await screen.findByText("t1")).toBeInTheDocument();

    // Expand the other three groups to exercise their item link branches.
    await userEvent.click(screen.getByRole("button", { name: /Volumes/ }));
    expect(await screen.findByText("v1")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /Functions/ }));
    expect(await screen.findByText("f1")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /Models/ }));
    expect(await screen.findByText("m1")).toBeInTheDocument();

    // The table item deep-links to its detail route.
    expect(screen.getByRole("link", { name: /t1/ })).toHaveAttribute(
      "href",
      "/catalog/main/default/table/t1",
    );
  });

  it("shows an empty state when there are no catalogs", async () => {
    renderWithProviders(<CatalogTree />, {
      rpc: { catalogs: { listCatalogs: () => ({ catalogs: [] }) } },
    });
    expect(await screen.findByText("No catalogs.")).toBeInTheDocument();
  });

  it("opens and highlights the active metric-view route", async () => {
    setParams({
      catalog: "main",
      schema: "default",
      view: "mv1",
    });
    renderWithProviders(<CatalogTree />, { rpc });

    const link = await screen.findByRole("link", { name: /mv1/ });
    expect(link).toHaveAttribute("href", "/catalog/main/default/view/mv1");
    expect(link).toHaveClass("bg-accent");
  });
});
