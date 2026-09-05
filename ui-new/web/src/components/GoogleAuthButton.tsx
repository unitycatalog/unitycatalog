import { useCallback, useEffect } from "react";

// GoogleAuthButton renders the Google Identity Services button and hands the
// resulting id_token to `onCredential`. The parent exchanges it for a UC session
// (token-exchange with ext=cookie). Mirrors the current ui's GoogleAuthButton.
export default function GoogleAuthButton({
  clientId,
  onCredential,
}: {
  clientId: string;
  onCredential: (idToken: string) => void;
}) {
  const handleCredential = useCallback(
    (res: { clientId?: string; credential?: string }) => {
      if (!res.credential) return;
      onCredential(res.credential);
    },
    [onCredential],
  );

  useEffect(() => {
    if (!clientId) return;
    const SRC = "https://accounts.google.com/gsi/client";

    const initialize = () => {
      const google = (window as unknown as { google?: any }).google;
      if (!google) return;
      google.accounts.id.initialize({ client_id: clientId, callback: handleCredential });
      google.accounts.id.renderButton(document.getElementById("google-client-button"), {
        text: "continue_with",
        width: 320,
        theme: "outline",
      });
    };

    const existing = Array.from(document.getElementsByTagName("script")).find((s) => s.src === SRC);
    if (existing) {
      initialize();
      return;
    }
    const script = document.createElement("script");
    script.src = SRC;
    script.async = true;
    script.defer = true;
    script.id = "google-client-script";
    script.onload = initialize;
    document.body.appendChild(script);

    return () => {
      (window as unknown as { google?: any }).google?.accounts.id.cancel();
    };
  }, [clientId, handleCredential]);

  return <div id="google-client-button" />;
}
