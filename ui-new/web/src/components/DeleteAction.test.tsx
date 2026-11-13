import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

const { toast } = vi.hoisted(() => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

vi.mock("sonner", () => ({ toast }));

import DeleteAction from "@/components/DeleteAction";

describe("DeleteAction", () => {
  beforeEach(() => {
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

  it("lets anyone delete once they enter the exact entity name", async () => {
    const onDelete = vi.fn().mockResolvedValue(undefined);
    render(
      <DeleteAction
        entityName="events"
        entityType="table"
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

  it("closes, reports success, and calls onDeleted after deleting", async () => {
    const onDeleted = vi.fn();
    render(
      <DeleteAction
        entityName="events"
        entityType="table"
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
      <DeleteAction
        entityName="events"
        entityType="table"
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
      <DeleteAction
        entityName="events"
        entityType="table"
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

  it("keeps focus in the dialog and returns it to the trigger on Escape", async () => {
    render(
      <DeleteAction
        entityName="events"
        entityType="table"
        onDelete={vi.fn()}
      />,
    );
    const trigger = screen.getByRole("button", { name: "Delete" });

    await userEvent.click(trigger);
    const dialog = screen.getByRole("dialog", { name: "Delete table?" });
    expect(screen.getByLabelText("Enter events to confirm")).toHaveFocus();
    for (let step = 0; step < 4; step += 1) {
      await userEvent.tab();
      expect(dialog).toContainElement(document.activeElement as HTMLElement);
    }

    await userEvent.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(trigger).toHaveFocus();
  });
});
