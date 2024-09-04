import { useCallback, useEffect, useMemo } from 'react';
import { Button } from 'antd';

export default function GoogleAuthButton({
  onGoogleSignIn,
}: {
  onGoogleSignIn: (credential: string) => void;
}) {
  const clientId = useMemo(() => process.env.REACT_APP_GOOGLE_CLIENT_ID, []);

  const handleGoogleSignIn = useCallback(
    (res: any) => {
      if (!res.clientId || !res.credential) return;
      onGoogleSignIn(res.credential);
    },
    [onGoogleSignIn],
  );

  useEffect(() => {
    const url = 'https://accounts.google.com/gsi/client';
    const scripts = document.getElementsByTagName('script');
    const isGsiScriptLoaded = Array.from(scripts).some(
      (script) => script.src === url,
    );

    if (isGsiScriptLoaded) return;

    const initializeGsi = () => {
      // Typescript will complain about window.google
      // Add types to your `react-app-env.d.ts` or //@ts-ignore it.
      if (!(window as any).google || isGsiScriptLoaded) return;

      (window as any).google.accounts.id.initialize({
        client_id: process.env.REACT_APP_GOOGLE_CLIENT_ID,
        callback: handleGoogleSignIn,
      });

      (window as any).google.accounts.id.renderButton(
        document.getElementById('google-client-button'),
        {
          text: 'continue_with', // customization attributes
          width: 240,
          theme: 'outline',
        },
      );
    };

    const script = document.createElement('script');
    script.src = 'https://accounts.google.com/gsi/client';
    script.onload = initializeGsi;
    script.defer = true;
    script.async = true;
    script.id = 'google-client-script';
    document.querySelector('body')?.appendChild(script);

    return () => {
      // Cleanup function that runs when component unmounts
      (window as any).google?.accounts.id.cancel();
      document.getElementById('google-client-script')?.remove();
    };
  }, [handleGoogleSignIn]);

  return (
    <>
      {clientId && <Button style={{ width: 240 }} id="google-client-button" />}
    </>
  );
}
