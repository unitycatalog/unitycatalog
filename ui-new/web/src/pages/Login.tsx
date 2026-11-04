import { useState } from "react";
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
  const { loginWithToken, signInWithAccessToken } = useAuth();
  const [error, setError] = useState<string | null>(null);
  const [pastedToken, setPastedToken] = useState("");

  const onGoogleCredential = async (idToken: string) => {
    setError(null);
    try {
      await loginWithToken(idToken);
      navigate({ to: "/" });
    } catch (e) {
      setError(e instanceof Error ? e.message : "Login failed. Contact your administrator.");
    }
  };

  const onPasteToken = () => {
    setError(null);
    if (!pastedToken.trim()) {
      setError("Paste a JWT access token.");
      return;
    }
    signInWithAccessToken(pastedToken);
    navigate({ to: "/" });
  };

  const googleClientId = appConfig?.googleClientId ?? "";
  const anyProvider = googleClientId || appConfig?.oktaEnabled || appConfig?.keycloakEnabled;

  return (
    <div className="flex min-h-full items-center justify-center bg-neutral-900 p-6">
      <div className="w-full max-w-md space-y-6">
        <div className="flex flex-col items-center gap-3">
          <Logo className="h-9 w-auto" title="Unity Catalog" />
          <p className="text-sm text-white/70">Sign in to Unity Catalog</p>
        </div>

        <Card>
          <CardHeader>
            <CardTitle className="text-base">Login</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {googleClientId && (
              <div className="flex justify-center">
                <GoogleAuthButton clientId={googleClientId} onCredential={onGoogleCredential} />
              </div>
            )}
            {appConfig?.oktaEnabled && (
              <p className="text-sm text-muted-foreground">
                Okta sign-in is enabled on the server. Complete the Okta flow, then return here.
              </p>
            )}
            {appConfig?.keycloakEnabled && (
              <p className="text-sm text-muted-foreground">
                Keycloak sign-in is enabled on the server. Complete the Keycloak flow, then return here.
              </p>
            )}
            {!anyProvider && (
              <p className="text-sm text-muted-foreground">
                No auth providers are enabled. Set them in the server configuration.
              </p>
            )}
            {error && (
              <p className="rounded-md bg-destructive/10 px-3 py-2 text-sm text-destructive">{error}</p>
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
                onKeyDown={(e) => e.key === "Enter" && onPasteToken()}
              />
            </div>
            <Button variant="outline" className="w-full" onClick={onPasteToken}>
              Use token &amp; continue
            </Button>
            <p className="text-xs text-muted-foreground">
              The token is sent as a bearer credential on every request and stored in this browser
              until you log out.
            </p>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
