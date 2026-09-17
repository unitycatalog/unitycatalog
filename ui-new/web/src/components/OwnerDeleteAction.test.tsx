import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

const { auth } = vi.hoisted(() => ({
  auth: {
    authEnabled: true,
    currentUser: { userName: "owner@example.com" },
  },
}));

vi.mock("@/context/auth-context", () => ({
  useOptionalAuth: () => auth,
}));

import OwnerDeleteAction from "@/components/OwnerDeleteAction";

describe("OwnerDeleteAction", () => {
  beforeEach(() => {
    auth.authEnabled = true;
    auth.currentUser = { userName: "owner@example.com" };
  });

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

  it("hides delete from non-owners", () => {
    auth.currentUser = { userName: "other@example.com" };
    render(
      <OwnerDeleteAction
        entityName="events"
        entityType="table"
        owner="owner@example.com"
        onDelete={vi.fn()}
      />,
    );
    expect(
      screen.queryByRole("button", { name: "Delete" }),
    ).not.toBeInTheDocument();
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
