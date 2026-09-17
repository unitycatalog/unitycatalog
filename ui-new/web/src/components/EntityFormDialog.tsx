import { useState, type FormEvent, type ReactNode } from "react";
import { Plus, X } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";

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
    <>
      <Button size="sm" onClick={() => setOpen(true)}>
        <Plus className="h-4 w-4" />
        {triggerLabel}
      </Button>
      {open && (
        <div
          className="fixed inset-0 z-50 grid place-items-center bg-black/50 p-4"
          role="presentation"
        >
          <form
            role="dialog"
            aria-modal="true"
            aria-labelledby="entity-form-title"
            className="max-h-[90vh] w-full max-w-lg overflow-auto rounded-lg border bg-background p-5 shadow-xl"
            onSubmit={submit}
          >
            <div className="flex items-center justify-between gap-4">
              <h2 id="entity-form-title" className="font-semibold">
                {title}
              </h2>
              <Button
                type="button"
                variant="ghost"
                size="icon"
                aria-label="Close"
                onClick={() => setOpen(false)}
              >
                <X className="h-4 w-4" />
              </Button>
            </div>
            <div className="mt-4 space-y-4">{children}</div>
            <div className="mt-5 flex justify-end gap-2">
              <Button
                type="button"
                variant="outline"
                onClick={() => setOpen(false)}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={saving}>
                {saving ? "Saving…" : submitLabel}
              </Button>
            </div>
          </form>
        </div>
      )}
    </>
  );
}
