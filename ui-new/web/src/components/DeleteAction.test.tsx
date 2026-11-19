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
    await userEvent.click(screen.getByRole("button", { name: "Delete table" }));
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

    await userEvent.click(screen.getByRole("button", { name: "Delete table" }));
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
    { entityType: "table", toastText: "Table deleted" },
    { entityType: "metric view", toastText: "Metric view deleted" },
    { entityType: "registered model", toastText: "Registered model deleted" },
  ])(
    "names the $entityType in its controls and reports success",
    async ({ entityType, toastText }) => {
      const onDeleted = vi.fn();
      render(
        <DeleteAction
          entityName="events"
          entityType={entityType}
          onDelete={vi.fn().mockResolvedValue(undefined)}
          onDeleted={onDeleted}
        />,
      );

      await userEvent.click(
        screen.getByRole("button", { name: `Delete ${entityType}` }),
      );
      expect(
        screen.getByRole("dialog", { name: `Delete ${entityType} events?` }),
      ).toBeInTheDocument();
      await userEvent.type(
        screen.getByLabelText("Enter events to confirm"),
        "events",
      );
      await userEvent.click(
        screen.getByRole("button", { name: "Delete permanently" }),
      );

      await waitFor(() => expect(onDeleted).toHaveBeenCalledOnce());
      expect(toast.success).toHaveBeenCalledWith(toastText);
      expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    },
  );

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

    await userEvent.click(screen.getByRole("button", { name: "Delete table" }));
    await userEvent.type(
      screen.getByLabelText("Enter events to confirm"),
      "events",
    );
    await userEvent.click(screen.getByRole("button", { name: "Cancel" }));
    await userEvent.click(screen.getByRole("button", { name: "Delete table" }));

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
    const trigger = screen.getByRole("button", { name: "Delete table" });

    await userEvent.click(trigger);
    const dialog = screen.getByRole("dialog", { name: "Delete table events?" });
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
