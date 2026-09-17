import EntityFormDialog from "@/components/EntityFormDialog";
import FormField from "@/components/FormField";
import { Input } from "@/components/ui/input";

export default function EditMetadataAction({
  name,
  comment,
  onSubmit,
}: {
  name: string;
  comment?: string;
  onSubmit: (changes: {
    newName?: string;
    comment?: string;
  }) => Promise<unknown>;
}) {
  return (
    <EntityFormDialog
      title="Edit metadata"
      triggerLabel="Edit"
      submitLabel="Save"
      onSubmit={(form) => {
        const newName = String(form.get("name"));
        const newComment = String(form.get("comment"));
        const changes = {
          newName: newName !== name ? newName : undefined,
          comment: newComment !== (comment ?? "") ? newComment : undefined,
        };
        if (changes.newName === undefined && changes.comment === undefined) {
          return Promise.resolve(false);
        }
        return onSubmit(changes);
      }}
    >
      <FormField id={`edit-${name}-name`} label="Name">
        <Input
          id={`edit-${name}-name`}
          name="name"
          required
          defaultValue={name}
        />
      </FormField>
      <FormField id={`edit-${name}-comment`} label="Comment">
        <Input
          id={`edit-${name}-comment`}
          name="comment"
          defaultValue={comment}
        />
      </FormField>
    </EntityFormDialog>
  );
}
