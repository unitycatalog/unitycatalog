import { useEffect, useState } from "react";
import { useNavigate } from "@tanstack/react-router";
import { useAppConfig } from "@/lib/appConfig";
import { useAuth } from "@/context/auth-context";
import Logo from "@/components/Logo";
import GoogleAuthButton from "@/components/GoogleAuthButton";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export default function Login() {
  const navigate = useNavigate();
  const { data: appConfig } = useAppConfig();
  const { authenticating, currentUser, loginWithToken, signInWithAccessToken } =
    useAuth();
  const [error, setError] = useState<string | null>(null);
  const [pastedToken, setPastedToken] = useState("");
  const [isVerifyingToken, setIsVerifyingToken] = useState(false);

  useEffect(() => {
    if (currentUser) {
      void navigate({ to: "/" });
    }
  }, [currentUser, navigate]);

  const onGoogleCredential = async (idToken: string) => {
    setError(null);
    try {
      await loginWithToken(idToken);
    } catch (e) {
      setError(
        e instanceof Error
          ? e.message
          : "Login failed. Contact your administrator.",
      );
    }
  };

  const onPasteToken = async () => {
    setError(null);
    if (!pastedToken.trim()) {
      setError("Paste a JWT access token.");
      return;
    }
    setIsVerifyingToken(true);
    try {
      await signInWithAccessToken(pastedToken);
    } catch (e) {
      const message =
        e instanceof Error ? e.message : "Token verification failed.";
      setError(`Unable to sign in: ${message}`);
    } finally {
      setIsVerifyingToken(false);
    }
  };

  const googleClientId = appConfig?.googleClientId ?? "";
  const anyProvider =
    googleClientId || appConfig?.oktaEnabled || appConfig?.keycloakEnabled;

  return (
    <div className="flex min-h-full items-center justify-center bg-neutral-900 p-6">
      <div className="w-full max-w-md space-y-6">
        <div className="flex flex-col items-center gap-3">
          <Logo className="h-9 w-auto" title="Unity Catalog" />
          <p className="text-sm text-white/70">Sign in to Unity Catalog</p>
        </div>

        <Card>
          <CardHeader>
            <CardTitle className="text-base">
              {authenticating ? "Authenticating" : "Login"}
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {authenticating ? (
              <div
                className="flex flex-col items-center gap-3 py-5 text-muted-foreground"
                role="status"
                aria-live="polite"
              >
                <div className="flex gap-2" aria-hidden="true">
                  <span className="h-2 w-2 animate-bounce rounded-full bg-current [animation-delay:-0.3s] motion-reduce:animate-none" />
                  <span className="h-2 w-2 animate-bounce rounded-full bg-current [animation-delay:-0.15s] motion-reduce:animate-none" />
                  <span className="h-2 w-2 animate-bounce rounded-full bg-current motion-reduce:animate-none" />
                </div>
                <p className="text-sm">Refreshing your session…</p>
              </div>
            ) : (
              <>
                {googleClientId && (
                  <div className="flex justify-center">
                    <GoogleAuthButton
                      clientId={googleClientId}
                      onCredential={onGoogleCredential}
                    />
                  </div>
                )}
                {appConfig?.oktaEnabled && (
                  <p className="text-sm text-muted-foreground">
                    Okta sign-in is enabled on the server. Complete the Okta
                    flow, then return here.
                  </p>
                )}
                {appConfig?.keycloakEnabled && (
                  <p className="text-sm text-muted-foreground">
                    Keycloak sign-in is enabled on the server. Complete the
                    Keycloak flow, then return here.
                  </p>
                )}
                {!anyProvider && (
                  <p className="text-sm text-muted-foreground">
                    No auth providers are enabled. Set them in the server
                    configuration.
                  </p>
                )}
                {error && (
                  <p className="rounded-md bg-destructive/10 px-3 py-2 text-sm text-destructive">
                    {error}
                  </p>
                )}
              </>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base">Or paste a token</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="space-y-1.5">
              <Label htmlFor="jwt">JWT access token</Label>
              <Input
                id="jwt"
                placeholder="eyJ..."
                value={pastedToken}
                onChange={(e) => setPastedToken(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault();
                    void onPasteToken();
                  }
                }}
              />
            </div>
            <Button
              variant="outline"
              className="w-full"
              disabled={isVerifyingToken}
              onClick={() => void onPasteToken()}
            >
              {isVerifyingToken ? "Verifying token…" : "Use token & continue"}
            </Button>
            <p className="text-xs text-muted-foreground">
              The token is sent as a bearer credential on every request and
              stored in this browser until you log out.
            </p>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
