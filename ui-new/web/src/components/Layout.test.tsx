import { describe, expect, it, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

vi.mock("@tanstack/react-router", () => import("@/test/tanstack-router-mock"));

const { logout, toastError } = vi.hoisted(() => ({
  logout: vi.fn(async () => {}),
  toastError: vi.fn(),
}));
vi.mock("sonner", async (importOriginal) => {
  const actual = await importOriginal<typeof import("sonner")>();
  return { ...actual, toast: { ...actual.toast, error: toastError } };
});
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

const emptyCatalogs = {
  catalogs: { listCatalogs: () => ({ catalogs: [] }) },
};

describe("Layout", () => {
  it("renders the chrome, auth badge, and children", async () => {
    renderWithProviders(
      <Layout>
        <div>page content</div>
      </Layout>,
      { rpc: emptyCatalogs },
    );
    expect(
      screen.getByRole("img", { name: "Unity Catalog" }),
    ).toBeInTheDocument();
    expect(screen.getByText("authenticated")).toBeInTheDocument();
    expect(screen.getByText("page content")).toBeInTheDocument();
    // CatalogTree mounted inside the sidebar.
    await waitFor(() =>
      expect(screen.getByText("No catalogs.")).toBeInTheDocument(),
    );
  });

  it("logs out from the sidebar button", async () => {
    renderWithProviders(<Layout>x</Layout>, { rpc: emptyCatalogs });
    await userEvent.click(screen.getByRole("button", { name: /^Log out$/ }));
    expect(logout).toHaveBeenCalled();
  });

  it("tells the user when Unity Catalog does not end the session", async () => {
    logout.mockRejectedValueOnce(new Error("UC request failed (HTTP 500): {}"));
    renderWithProviders(<Layout>x</Layout>, { handler: emptyCatalogs });
    await userEvent.click(screen.getByRole("button", { name: /^Log out$/ }));
    await waitFor(() =>
      expect(toastError).toHaveBeenCalledWith("Sign out failed: UC request failed (HTTP 500): {}"),
    );
  });

  it("collapses the navigation", async () => {
    renderWithProviders(<Layout>x</Layout>, { rpc: emptyCatalogs });
    const toggle = screen.getByRole("button", { name: /Collapse navigation/ });
    await userEvent.click(toggle);
    expect(
      screen.getByRole("button", { name: /Expand navigation/ }),
    ).toBeInTheDocument();
  });
});
