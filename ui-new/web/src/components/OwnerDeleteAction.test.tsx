import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

const { auth, toast } = vi.hoisted(() => ({
  auth: {
    authEnabled: true,
    currentUser: { userName: "owner@example.com" } as {
      userName: string;
    } | null,
  },
  toast: { success: vi.fn(), error: vi.fn() },
}));

vi.mock("sonner", () => ({ toast }));

vi.mock("@/context/auth-context", () => ({
  useOptionalAuth: () => auth,
}));

import OwnerDeleteAction from "@/components/OwnerDeleteAction";

describe("OwnerDeleteAction", () => {
  beforeEach(() => {
    auth.authEnabled = true;
    auth.currentUser = { userName: "owner@example.com" };
    toast.success.mockReset();
    toast.error.mockReset();
  });

  async function confirmDelete() {
    await userEvent.click(screen.getByRole("button", { name: "Delete" }));
    await userEvent.type(
      screen.getByLabelText("Enter events to confirm"),
      "events",
    );
    await userEvent.click(
      screen.getByRole("button", { name: "Delete permanently" }),
    );
  }

  it("requires the owner to enter the exact entity name", async () => {
    const onDelete = vi.fn().mockResolvedValue(undefined);
    render(
      <OwnerDeleteAction
        entityName="events"
        entityType="table"
        owner="owner@example.com"
        onDelete={onDelete}
      />,
    );

    await userEvent.click(screen.getByRole("button", { name: "Delete" }));
    const confirm = screen.getByRole("button", { name: "Delete permanently" });
    expect(confirm).toBeDisabled();

    await userEvent.type(
      screen.getByLabelText("Enter events to confirm"),
      "event",
    );
    expect(confirm).toBeDisabled();
    await userEvent.type(screen.getByLabelText("Enter events to confirm"), "s");
    await userEvent.click(confirm);

    await waitFor(() => expect(onDelete).toHaveBeenCalledOnce());
  });

  it.each([
    {
      when: "auth is disabled",
      authEnabled: false,
      user: null,
      owner: undefined,
      visible: true,
    },
    {
      when: "the user owns it",
      authEnabled: true,
      user: "owner@example.com",
      owner: "owner@example.com",
      visible: true,
    },
    {
      when: "another user owns it",
      authEnabled: true,
      user: "other@example.com",
      owner: "owner@example.com",
      visible: false,
    },
    {
      when: "the owner is unknown",
      authEnabled: true,
      user: "owner@example.com",
      owner: undefined,
      visible: false,
    },
    {
      when: "nobody is signed in",
      authEnabled: true,
      user: null,
      owner: "owner@example.com",
      visible: false,
    },
  ])(
    "shows delete only to the owner: $when",
    ({ authEnabled, user, owner, visible }) => {
      auth.authEnabled = authEnabled;
      auth.currentUser = user ? { userName: user } : null;
      render(
        <OwnerDeleteAction
          entityName="events"
          entityType="table"
          owner={owner}
          onDelete={vi.fn()}
        />,
      );
      expect(screen.queryByRole("button", { name: "Delete" }) !== null).toBe(
        visible,
      );
    },
  );

  it("closes, reports success, and calls onDeleted after deleting", async () => {
    const onDeleted = vi.fn();
    render(
      <OwnerDeleteAction
        entityName="events"
        entityType="table"
        owner="owner@example.com"
        onDelete={vi.fn().mockResolvedValue(undefined)}
        onDeleted={onDeleted}
      />,
    );

    await confirmDelete();

    await waitFor(() => expect(onDeleted).toHaveBeenCalledOnce());
    expect(toast.success).toHaveBeenCalledWith("table deleted");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("keeps the dialog open and shows the error when delete fails", async () => {
    const onDeleted = vi.fn();
    render(
      <OwnerDeleteAction
        entityName="events"
        entityType="table"
        owner="owner@example.com"
        onDelete={vi.fn().mockRejectedValue(new Error("PERMISSION_DENIED"))}
        onDeleted={onDeleted}
      />,
    );

    await confirmDelete();

    await waitFor(() =>
      expect(toast.error).toHaveBeenCalledWith("PERMISSION_DENIED"),
    );
    expect(screen.getByRole("dialog")).toBeInTheDocument();
    expect(onDeleted).not.toHaveBeenCalled();
    expect(toast.success).not.toHaveBeenCalled();
  });

  it("clears confirmation after cancellation", async () => {
    render(
      <OwnerDeleteAction
        entityName="events"
        entityType="table"
        owner="owner@example.com"
        onDelete={vi.fn()}
      />,
    );

    await userEvent.click(screen.getByRole("button", { name: "Delete" }));
    await userEvent.type(
      screen.getByLabelText("Enter events to confirm"),
      "events",
    );
    await userEvent.click(screen.getByRole("button", { name: "Cancel" }));
    await userEvent.click(screen.getByRole("button", { name: "Delete" }));

    expect(screen.getByLabelText("Enter events to confirm")).toHaveValue("");
    expect(
      screen.getByRole("button", { name: "Delete permanently" }),
    ).toBeDisabled();
  });
});
