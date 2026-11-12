import { useEffect, useState } from "react";
import { useNavigate } from "@tanstack/react-router";
import { useGetCurrentUser } from "@/hooks/user";
import { useAppConfig } from "@/lib/appConfig";
import { useAuth } from "@/context/auth-context";
import { beginOktaLogin, canRetryOktaSignIn, isOktaToken, markOktaToken, oktaConfigFrom } from "@/lib/okta";
import { clearToken, getToken } from "@/lib/session";
import Logo from "@/components/Logo";
import GoogleAuthButton from "@/components/GoogleAuthButton";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export default function Login() {
  const navigate = useNavigate();
  const { data: appConfig } = useAppConfig();
  const { loginWithToken, signInWithAccessToken, currentUser } = useAuth();
  // Read-only subscription, so the check below re-runs when a background refetch settles.
  const { status, fetchStatus, data: user } = useGetCurrentUser(false);
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

  const okta = appConfig ? oktaConfigFrom(appConfig) : null;

  const onOktaSignIn = async () => {
    if (!okta) return;
    setError(null);
    try {
      window.location.assign(await beginOktaLogin(okta));
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not start the Okta sign-in.");
    }
  };

  // An Okta token that is still stored while we are on the login page was
  // refused (expired): sign in with Okta again, but only once in a while. Act
  // only on a settled 401 from /scim2/Me (the cached user is null and idle), not
  // on a manual visit, a request in flight or a 5xx.
  useEffect(() => {
    if (!okta || !getToken() || !isOktaToken()) return;
    if (status !== "success" || user !== null || fetchStatus !== "idle") return;
    clearToken();
    markOktaToken(false);
    if (canRetryOktaSignIn()) void onOktaSignIn();
    else setError("Unity Catalog refused your Okta sign-in. Contact your administrator.");
  }, [okta?.issuer, okta?.clientId, status, fetchStatus, user]);

  useEffect(() => {
    if (currentUser) navigate({ to: "/", replace: true });
  }, [currentUser]);

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
            {okta && (
              <Button className="w-full" onClick={onOktaSignIn}>
                Sign in with Okta
              </Button>
            )}
            {appConfig?.oktaEnabled && !okta && (
              <p className="text-sm text-muted-foreground">
                Okta sign-in is enabled on the server, but OKTA_ISSUER and OKTA_CLIENT_ID are not both set.
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
