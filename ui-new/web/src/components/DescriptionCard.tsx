import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

// DescriptionCard shows an object's free-text comment, or a muted placeholder
// when none is set. Read-only for this milestone (inline editing is deferred).
export default function DescriptionCard({ comment }: { comment?: string }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm">Description</CardTitle>
      </CardHeader>
      <CardContent>
        {comment ? (
          <p className="text-sm text-foreground">{comment}</p>
        ) : (
          <p className="text-sm text-muted-foreground">No description provided.</p>
        )}
      </CardContent>
    </Card>
  );
}
