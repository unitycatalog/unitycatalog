import { describe, expect, it, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

vi.mock("@tanstack/react-router", () => import("@/test/tanstack-router-mock"));

const { logout } = vi.hoisted(() => ({ logout: vi.fn() }));
vi.mock("@/context/auth-context", () => ({
  useAuth: () => ({
    authEnabled: true,
    loading: false,
    currentUser: { displayName: "Ada", emails: [{ value: "ada@x.io" }] },
    hasAccessToken: false,
    loginWithToken: vi.fn(),
    signInWithAccessToken: vi.fn(),
    logout,
  }),
}));

import Layout from "@/components/Layout";
import { renderWithProviders } from "@/test/providers";

const emptyCatalogs = () => ({ httpStatus: 200, ok: true, body: JSON.stringify({ catalogs: [] }) });

describe("Layout", () => {
  it("renders the chrome, auth badge, and children", async () => {
    renderWithProviders(
      <Layout>
        <div>page content</div>
      </Layout>,
      { handler: emptyCatalogs },
    );
    expect(screen.getByRole("img", { name: "Unity Catalog" })).toBeInTheDocument();
    expect(screen.getByText("authenticated")).toBeInTheDocument();
    expect(screen.getByText("page content")).toBeInTheDocument();
    // CatalogTree mounted inside the sidebar.
    await waitFor(() => expect(screen.getByText("No catalogs.")).toBeInTheDocument());
  });

  it("logs out from the sidebar button", async () => {
    renderWithProviders(<Layout>x</Layout>, { handler: emptyCatalogs });
    await userEvent.click(screen.getByRole("button", { name: /^Log out$/ }));
    expect(logout).toHaveBeenCalled();
  });

  it("collapses the navigation", async () => {
    renderWithProviders(<Layout>x</Layout>, { handler: emptyCatalogs });
    const toggle = screen.getByRole("button", { name: /Collapse navigation/ });
    await userEvent.click(toggle);
    expect(screen.getByRole("button", { name: /Expand navigation/ })).toBeInTheDocument();
  });
});
