import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

vi.mock("@tanstack/react-router", () => import("@/test/tanstack-router-mock"));

const { signInWithAccessToken, loginWithToken } = vi.hoisted(() => ({
  signInWithAccessToken: vi.fn(),
  loginWithToken: vi.fn().mockResolvedValue(undefined),
}));
vi.mock("@/context/auth-context", () => ({
  useAuth: () => ({ loginWithToken, signInWithAccessToken }),
}));

const { appConfig } = vi.hoisted(() => ({ appConfig: { current: {} as Record<string, unknown> } }));
vi.mock("@/lib/appConfig", () => ({
  useAppConfig: () => ({ data: appConfig.current }),
}));

import Login from "@/pages/Login";
import { navigateMock } from "@/test/tanstack-router-mock";

beforeEach(() => {
  navigateMock.mockReset();
  signInWithAccessToken.mockReset();
  appConfig.current = {};
});

describe("Login", () => {
  it("shows a no-providers message when nothing is enabled", () => {
    render(<Login />);
    expect(screen.getByText(/No auth providers are enabled/)).toBeInTheDocument();
  });

  it("advertises Okta and Keycloak when enabled", () => {
    appConfig.current = { oktaEnabled: true, keycloakEnabled: true };
    render(<Login />);
    expect(screen.getByText(/Okta sign-in is enabled/)).toBeInTheDocument();
    expect(screen.getByText(/Keycloak sign-in is enabled/)).toBeInTheDocument();
  });

  it("validates an empty pasted token", async () => {
    render(<Login />);
    await userEvent.click(screen.getByRole("button", { name: /Use token/ }));
    expect(screen.getByText("Paste a JWT access token.")).toBeInTheDocument();
    expect(signInWithAccessToken).not.toHaveBeenCalled();
  });

  it("signs in with a pasted token and navigates home", async () => {
    render(<Login />);
    await userEvent.type(screen.getByLabelText("JWT access token"), "jwt-abc");
    await userEvent.click(screen.getByRole("button", { name: /Use token/ }));
    expect(signInWithAccessToken).toHaveBeenCalledWith("jwt-abc");
    expect(navigateMock).toHaveBeenCalledWith({ to: "/" });
  });
});
