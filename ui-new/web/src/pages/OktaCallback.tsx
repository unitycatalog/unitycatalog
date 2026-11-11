import { useEffect, useRef, useState } from "react";
import { Link, useNavigate } from "@tanstack/react-router";
import { useAppConfig } from "@/lib/appConfig";
import { useAuth } from "@/context/auth-context";
import { exchangeCode, oktaConfigFrom, takeCallback } from "@/lib/okta";
import Logo from "@/components/Logo";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

// Where Okta returns after "Sign in with Okta": checks the state, trades the
// code (with the PKCE verifier) for tokens, and finishes the sign-in.
export default function OktaCallback() {
  const navigate = useNavigate();
  const { data: appConfig } = useAppConfig();
  const { signInWithOkta } = useAuth();
  const [error, setError] = useState<string | null>(null);
  // The callback must run once: it consumes the pending sign-in.
  const started = useRef(false);

  useEffect(() => {
    if (!appConfig || started.current) return;
    started.current = true;
    const okta = oktaConfigFrom(appConfig);
    (async () => {
      if (!okta) throw new Error("Okta sign-in is not configured on the server.");
      const { code, verifier } = takeCallback(window.location.search);
      await signInWithOkta(await exchangeCode(okta, code, verifier));
      navigate({ to: "/" });
    })().catch((e) => setError(e instanceof Error ? e.message : "Okta sign-in failed."));
  }, [appConfig, navigate, signInWithOkta]);

  return (
    <div className="flex min-h-full items-center justify-center bg-neutral-900 p-6">
      <div className="w-full max-w-md space-y-6">
        <div className="flex justify-center">
          <Logo className="h-9 w-auto" title="Unity Catalog" />
        </div>
        <Card>
          <CardHeader>
            <CardTitle className="text-base">{error ? "Okta sign-in failed" : "Signing in with Okta"}</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm">
            {error ? (
              <>
                <p className="rounded-md bg-destructive/10 px-3 py-2 text-destructive">{error}</p>
                <Link to="/login" className="underline">
                  Back to login
                </Link>
              </>
            ) : (
              <p className="text-muted-foreground">Finishing the sign-in...</p>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
