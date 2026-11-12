import { useState, type FormEvent, type ReactNode } from "react";
import { Plus } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogFooter,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";

export default function EntityFormDialog({
  title,
  triggerLabel,
  submitLabel = "Create",
  children,
  onSubmit,
}: {
  title: string;
  triggerLabel: string;
  submitLabel?: string;
  children: ReactNode;
  onSubmit: (form: FormData) => Promise<unknown>;
}) {
  const [open, setOpen] = useState(false);
  const [saving, setSaving] = useState(false);

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setSaving(true);
    try {
      const result = await onSubmit(new FormData(event.currentTarget));
      if (result === false) return;
      toast.success(`${title} complete`);
      setOpen(false);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : `${title} failed`);
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button size="sm">
          <Plus className="h-4 w-4" />
          {triggerLabel}
        </Button>
      </DialogTrigger>
      <DialogContent
        aria-describedby={undefined}
        className="max-h-[90vh] overflow-auto"
      >
        <form onSubmit={submit}>
          <DialogTitle>{title}</DialogTitle>
          <div className="mt-4 space-y-4">{children}</div>
          <DialogFooter className="mt-5">
            <DialogClose asChild>
              <Button type="button" variant="outline">
                Cancel
              </Button>
            </DialogClose>
            <Button type="submit" disabled={saving}>
              {saving ? "Saving…" : submitLabel}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
