import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

vi.mock("@tanstack/react-router", () => import("@/test/tanstack-router-mock"));

const { authState, signInWithAccessToken, loginWithToken } = vi.hoisted(() => ({
  authState: {
    current: {
      authenticating: false,
      currentUser: null as { displayName: string } | null,
    },
  },
  signInWithAccessToken: vi.fn(),
  loginWithToken: vi.fn().mockResolvedValue(undefined),
}));
vi.mock("@/context/auth-context", () => ({
  useAuth: () => ({
    authenticating: authState.current.authenticating,
    currentUser: authState.current.currentUser,
    loginWithToken,
    signInWithAccessToken,
  }),
}));

const { appConfig } = vi.hoisted(() => ({
  appConfig: { current: {} as Record<string, unknown> },
}));
vi.mock("@/lib/appConfig", () => ({
  useAppConfig: () => ({ data: appConfig.current }),
}));

import Login from "@/pages/Login";
import { navigateMock } from "@/test/tanstack-router-mock";

beforeEach(() => {
  navigateMock.mockReset();
  loginWithToken.mockReset();
  loginWithToken.mockResolvedValue(undefined);
  signInWithAccessToken.mockReset();
  signInWithAccessToken.mockResolvedValue(undefined);
  authState.current.authenticating = false;
  authState.current.currentUser = null;
  appConfig.current = {};
});

describe("Login", () => {
  it("shows a no-providers message when nothing is enabled", () => {
    render(<Login />);
    expect(
      screen.getByText(/No auth providers are enabled/),
    ).toBeInTheDocument();
  });

  it("advertises Okta and Keycloak when enabled", () => {
    appConfig.current = { oktaEnabled: true, keycloakEnabled: true };
    render(<Login />);
    expect(screen.getByText(/Okta sign-in is enabled/)).toBeInTheDocument();
    expect(screen.getByText(/Keycloak sign-in is enabled/)).toBeInTheDocument();
  });

  it("shows progress while the authenticated session is refreshed", () => {
    authState.current.authenticating = true;
    render(<Login />);

    expect(screen.getByText("Authenticating")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(
      "Refreshing your session",
    );
  });

  it("validates an empty pasted token", async () => {
    render(<Login />);
    await userEvent.click(screen.getByRole("button", { name: /Use token/ }));
    expect(screen.getByText("Paste a JWT access token.")).toBeInTheDocument();
    expect(signInWithAccessToken).not.toHaveBeenCalled();
  });

  it("waits for the authenticated user before navigating home", async () => {
    const { rerender } = render(<Login />);
    await userEvent.type(screen.getByLabelText("JWT access token"), "jwt-abc");
    await userEvent.click(screen.getByRole("button", { name: /Use token/ }));
    expect(signInWithAccessToken).toHaveBeenCalledWith("jwt-abc");
    expect(navigateMock).not.toHaveBeenCalled();

    authState.current.currentUser = { displayName: "Ada" };
    rerender(<Login />);
    await waitFor(() => expect(navigateMock).toHaveBeenCalledWith({ to: "/" }));
  });

  it("shows pasted-token verification failures without navigating", async () => {
    signInWithAccessToken.mockRejectedValueOnce(
      new Error("Unity Catalog server is unavailable"),
    );
    render(<Login />);

    await userEvent.type(screen.getByLabelText("JWT access token"), "jwt-abc");
    await userEvent.click(screen.getByRole("button", { name: /Use token/ }));

    expect(
      await screen.findByText(/Unity Catalog server is unavailable/),
    ).toBeInTheDocument();
    expect(navigateMock).not.toHaveBeenCalled();
  });
});
