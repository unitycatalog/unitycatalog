import { useState } from "react";
import { Trash2 } from "lucide-react";
import { toast } from "sonner";
import { useOptionalAuth } from "@/context/auth-context";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
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
    <Dialog open={open} onOpenChange={setDialogOpen}>
      <DialogTrigger asChild>
        <Button variant="destructive" size="sm">
          <Trash2 className="h-4 w-4" />
          Delete
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Delete {entityType}?</DialogTitle>
          <DialogDescription>
            This cannot be undone. Enter <strong>{entityName}</strong> to
            confirm.
          </DialogDescription>
        </DialogHeader>
        <Input
          value={confirmation}
          onChange={(event) => setConfirmation(event.target.value)}
          aria-label={`Enter ${entityName} to confirm`}
          autoComplete="off"
        />
        <DialogFooter>
          <DialogClose asChild>
            <Button variant="outline">Cancel</Button>
          </DialogClose>
          <Button
            variant="destructive"
            disabled={confirmation !== entityName || deleting}
            onClick={remove}
          >
            {deleting ? "Deleting…" : "Delete permanently"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
