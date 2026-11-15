import { expect, it, vi } from "vitest";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
const { toastError } = vi.hoisted(() => ({ toastError: vi.fn() }));
vi.mock("sonner", async (importOriginal) => {
  const actual = await importOriginal<typeof import("sonner")>();
  return { ...actual, toast: { ...actual.toast, error: toastError } };
});

import EntityFormDialog from "@/components/EntityFormDialog";

it.each(["Cancel", "Escape"])(
  "keeps a draft reopened after %s when an earlier save resolves",
  async (closeWith) => {
    const save = Promise.withResolvers<unknown>();
    const onSubmit = vi.fn(() => save.promise);
    render(
      <EntityFormDialog
        title="Create thing"
        triggerLabel="New"
        onSubmit={onSubmit}
      >
        <input aria-label="Name" name="name" />
      </EntityFormDialog>,
    );

    await userEvent.click(screen.getByRole("button", { name: "New" }));
    await userEvent.type(screen.getByLabelText("Name"), "first draft");
    await userEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => expect(onSubmit).toHaveBeenCalledOnce());

    if (closeWith === "Escape") await userEvent.keyboard("{Escape}");
    else await userEvent.click(screen.getByRole("button", { name: "Cancel" }));
    await userEvent.click(screen.getByRole("button", { name: "New" }));
    const name = screen.getByLabelText("Name");
    await userEvent.type(name, "new draft");

    await act(async () => {
      save.resolve(undefined);
      await save.promise;
    });

    expect(screen.getByRole("dialog")).toBeInTheDocument();
    expect(name).toHaveValue("new draft");
  },
);

it("keeps the dialog open and reports a failed save", async () => {
  const onSubmit = vi.fn().mockRejectedValue(new Error("save failed"));
  render(
    <EntityFormDialog
      title="Create thing"
      triggerLabel="New"
      onSubmit={onSubmit}
    >
      <input aria-label="Name" name="name" />
    </EntityFormDialog>,
  );

  await userEvent.click(screen.getByRole("button", { name: "New" }));
  await userEvent.click(screen.getByRole("button", { name: "Create" }));

  await waitFor(() => expect(toastError).toHaveBeenCalledWith("save failed"));
  expect(screen.getByRole("dialog")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Create" })).toBeEnabled();
});

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
