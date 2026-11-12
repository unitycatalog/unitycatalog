import { expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import EntityFormDialog from "@/components/EntityFormDialog";

it("keeps focus in the form and returns it to the trigger on Escape", async () => {
  render(
    <EntityFormDialog
      title="Create thing"
      triggerLabel="New"
      onSubmit={vi.fn()}
    >
      <input aria-label="Name" name="name" />
    </EntityFormDialog>,
  );
  const trigger = screen.getByRole("button", { name: "New" });

  await userEvent.click(trigger);
  const dialog = screen.getByRole("dialog", { name: "Create thing" });
  expect(screen.getByLabelText("Name")).toHaveFocus();
  for (let step = 0; step < 4; step += 1) {
    await userEvent.tab();
    expect(dialog).toContainElement(document.activeElement as HTMLElement);
  }

  await userEvent.keyboard("{Escape}");
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  expect(trigger).toHaveFocus();
});
