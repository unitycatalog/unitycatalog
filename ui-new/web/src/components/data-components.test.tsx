import { describe, expect, it, vi, beforeEach } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

vi.mock("@tanstack/react-router", () => import("@/test/tanstack-router-mock"));

import { setParams } from "@/test/tanstack-router-mock";
import { renderWithProviders, type UcHandler } from "@/test/providers";
import PermissionsPanel from "@/components/PermissionsPanel";
import CatalogTree from "@/components/CatalogTree";

beforeEach(() => setParams({}));

describe("PermissionsPanel", () => {
  const handler: UcHandler = ({ path }) =>
    path.includes("/permissions/")
      ? {
          httpStatus: 200,
          ok: true,
          body: JSON.stringify({ privilege_assignments: [{ principal: "ada@x.io", privileges: ["SELECT", "MODIFY"] }] }),
        }
      : { httpStatus: 404, ok: false, body: "{}" };

  it("renders principals and their privileges", async () => {
    renderWithProviders(<PermissionsPanel securableType="table" fullName="main.default.events" />, { handler });
    await waitFor(() => expect(screen.getByText("ada@x.io")).toBeInTheDocument());
    expect(screen.getByText("SELECT")).toBeInTheDocument();
    expect(screen.getByText("MODIFY")).toBeInTheDocument();
  });

  it("shows a placeholder when there are no grants", async () => {
    renderWithProviders(<PermissionsPanel securableType="catalog" fullName="main" />, {
      handler: () => ({ httpStatus: 200, ok: true, body: JSON.stringify({ privilege_assignments: [] }) }),
    });
    await waitFor(() => expect(screen.getByText("No privileges granted.")).toBeInTheDocument());
  });
});

describe("CatalogTree", () => {
  const handler: UcHandler = ({ path }) => {
    const body = (o: unknown) => ({ httpStatus: 200, ok: true, body: JSON.stringify(o) });
    if (path === "/api/2.1/unity-catalog/catalogs") return body({ catalogs: [{ name: "main" }] });
    if (path === "/api/2.1/unity-catalog/schemas") return body({ schemas: [{ name: "default" }] });
    if (path === "/api/2.1/unity-catalog/tables") return body({ tables: [{ name: "t1" }] });
    if (path === "/api/2.1/unity-catalog/volumes") return body({ volumes: [{ name: "v1" }] });
    if (path === "/api/2.1/unity-catalog/functions") return body({ functions: [{ name: "f1" }] });
    if (path === "/api/2.1/unity-catalog/models") return body({ registered_models: [{ name: "m1" }] });
    return { httpStatus: 404, ok: false, body: "{}" };
  };

  it("auto-expands the active catalog/schema and lazy-loads each group", async () => {
    // Seed the route so the catalog + schema nodes open on mount and Tables
    // (defaultOpen) loads without a click.
    setParams({ catalog: "main", schema: "default" });
    renderWithProviders(<CatalogTree />, { handler });

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
      handler: () => ({ httpStatus: 200, ok: true, body: JSON.stringify({ catalogs: [] }) }),
    });
    expect(await screen.findByText("No catalogs.")).toBeInTheDocument();
  });
});
