import { useState } from "react";
import { Trash2, X } from "lucide-react";
import { toast } from "sonner";
import { useOptionalAuth } from "@/context/auth-context";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

export default function OwnerDeleteAction({
  entityName,
  entityType,
  owner,
  onDelete,
  onDeleted,
}: {
  entityName: string;
  entityType: string;
  owner?: string;
  onDelete: () => Promise<unknown>;
  onDeleted?: () => void;
}) {
  const auth = useOptionalAuth();
  const [open, setOpen] = useState(false);
  const [confirmation, setConfirmation] = useState("");
  const [deleting, setDeleting] = useState(false);
  const isOwner =
    !!auth &&
    (!auth.authEnabled || (!!owner && auth.currentUser?.userName === owner));

  if (!isOwner) return null;

  const setDialogOpen = (nextOpen: boolean) => {
    setConfirmation("");
    setOpen(nextOpen);
  };

  const remove = async () => {
    setDeleting(true);
    try {
      await onDelete();
      toast.success(`${entityType} deleted`);
      setDialogOpen(false);
      onDeleted?.();
    } catch (error) {
      toast.error(
        error instanceof Error
          ? error.message
          : `Failed to delete ${entityType}`,
      );
    } finally {
      setDeleting(false);
    }
  };

  return (
    <>
      <Button
        variant="destructive"
        size="sm"
        onClick={() => setDialogOpen(true)}
      >
        <Trash2 className="h-4 w-4" />
        Delete
      </Button>
      {open && (
        <div
          className="fixed inset-0 z-50 grid place-items-center bg-black/50 p-4"
          role="presentation"
        >
          <div
            role="dialog"
            aria-modal="true"
            aria-labelledby="delete-title"
            className="w-full max-w-md rounded-lg border bg-background p-5 shadow-xl"
          >
            <div className="flex items-start justify-between gap-4">
              <div>
                <h2 id="delete-title" className="font-semibold">
                  Delete {entityType}?
                </h2>
                <p className="mt-1 text-sm text-muted-foreground">
                  This cannot be undone. Enter <strong>{entityName}</strong> to
                  confirm.
                </p>
              </div>
              <Button
                variant="ghost"
                size="icon"
                aria-label="Close"
                onClick={() => setDialogOpen(false)}
              >
                <X className="h-4 w-4" />
              </Button>
            </div>
            <Input
              className="mt-4"
              value={confirmation}
              onChange={(event) => setConfirmation(event.target.value)}
              aria-label={`Enter ${entityName} to confirm`}
              autoComplete="off"
            />
            <div className="mt-4 flex justify-end gap-2">
              <Button variant="outline" onClick={() => setDialogOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="destructive"
                disabled={confirmation !== entityName || deleting}
                onClick={remove}
              >
                {deleting ? "Deleting…" : "Delete permanently"}
              </Button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
