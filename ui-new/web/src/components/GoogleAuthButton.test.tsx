import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, waitFor } from "@testing-library/react";
import GoogleAuthButton from "@/components/GoogleAuthButton";

beforeEach(() => {
  document.head.innerHTML = "";
  document.body.innerHTML = "";
  delete (window as unknown as { google?: unknown }).google;
});

describe("GoogleAuthButton", () => {
  it("does nothing without a client id", () => {
    const { container } = render(<GoogleAuthButton clientId="" onCredential={() => {}} />);
    // Renders the placeholder container but injects no GSI script.
    expect(container.querySelector("#google-client-button")).toBeTruthy();
    expect(document.getElementById("google-client-script")).toBeNull();
  });

  it("initializes GSI and renders the button when the script loads", async () => {
    const initialize = vi.fn();
    const renderButton = vi.fn();
    // Simulate the GSI global becoming available.
    (window as unknown as { google: unknown }).google = {
      accounts: { id: { initialize, renderButton, cancel: vi.fn() } },
    };

    render(<GoogleAuthButton clientId="client-123" onCredential={() => {}} />);

    const script = document.getElementById("google-client-script") as HTMLScriptElement;
    expect(script).toBeTruthy();
    // Fire the script onload to trigger initialize/renderButton.
    script.onload?.(new Event("load"));

    await waitFor(() => expect(initialize).toHaveBeenCalled());
    expect(initialize.mock.calls[0][0]).toMatchObject({ client_id: "client-123" });
    expect(renderButton).toHaveBeenCalled();
  });

  it("forwards the credential from the GSI callback", async () => {
    const onCredential = vi.fn();
    let captured: ((res: { credential?: string }) => void) | undefined;
    (window as unknown as { google: unknown }).google = {
      accounts: {
        id: {
          initialize: (opts: { callback: (r: { credential?: string }) => void }) => {
            captured = opts.callback;
          },
          renderButton: vi.fn(),
          cancel: vi.fn(),
        },
      },
    };

    render(<GoogleAuthButton clientId="client-123" onCredential={onCredential} />);
    const script = document.getElementById("google-client-script") as HTMLScriptElement;
    script.onload?.(new Event("load"));
    await waitFor(() => expect(captured).toBeTypeOf("function"));

    captured?.({ credential: "id-token-xyz" });
    expect(onCredential).toHaveBeenCalledWith("id-token-xyz");
    // A callback without a credential is ignored.
    captured?.({});
    expect(onCredential).toHaveBeenCalledTimes(1);
  });
});
