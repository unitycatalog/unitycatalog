import { expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import EditMetadataAction from "@/components/EditMetadataAction";

it("submits only changed metadata", async () => {
  const onSubmit = vi.fn().mockResolvedValue(undefined);
  render(
    <EditMetadataAction name="events" comment="old" onSubmit={onSubmit} />,
  );

  await userEvent.click(screen.getByRole("button", { name: "Edit" }));
  const name = screen.getByLabelText("Name");
  const comment = screen.getByLabelText("Comment");
  await userEvent.clear(name);
  await userEvent.type(name, "events_v2");
  await userEvent.clear(comment);
  await userEvent.type(comment, "new");
  await userEvent.click(screen.getByRole("button", { name: "Save" }));

  await waitFor(() =>
    expect(onSubmit).toHaveBeenCalledWith({
      newName: "events_v2",
      comment: "new",
    }),
  );
});

it("does not submit unchanged metadata", async () => {
  const onSubmit = vi.fn().mockResolvedValue(undefined);
  render(
    <EditMetadataAction name="events" comment="old" onSubmit={onSubmit} />,
  );

  await userEvent.click(screen.getByRole("button", { name: "Edit" }));
  await userEvent.click(screen.getByRole("button", { name: "Save" }));

  expect(onSubmit).not.toHaveBeenCalled();
  expect(screen.getByRole("dialog")).toBeInTheDocument();
});

it("submits an empty comment to clear existing metadata", async () => {
  const onSubmit = vi.fn().mockResolvedValue(undefined);
  render(
    <EditMetadataAction name="events" comment="old" onSubmit={onSubmit} />,
  );

  await userEvent.click(screen.getByRole("button", { name: "Edit" }));
  await userEvent.clear(screen.getByLabelText("Comment"));
  await userEvent.click(screen.getByRole("button", { name: "Save" }));

  await waitFor(() =>
    expect(onSubmit).toHaveBeenCalledWith({
      newName: undefined,
      comment: "",
    }),
  );
});
