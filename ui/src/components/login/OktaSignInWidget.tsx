import { useEffect } from 'react';
import OktaSignIn from '@okta/okta-signin-widget';

interface OktaSignInWidgetProps {
  onSuccess: (tokens: any) => void;
  onError: (error: Error) => void;
}

export default function OktaSignInWidget({
  onSuccess,
  onError,
}: OktaSignInWidgetProps) {
  useEffect(() => {
    const widget = new OktaSignIn({
      clientId: process.env.REACT_APP_OKTA_CLIENT_ID,
      redirectUri: window.location.origin,
      issuer:
        'https://' + process.env.REACT_APP_OKTA_DOMAIN + '/oauth2/default',
      logo: '/uc-logo.png',
    });
    widget
      .showSignInToGetTokens({
        el: '#osw-container',
      })
      .then(function (res) {
        onSuccess(res?.idToken?.idToken);
        widget.remove();
      })
      .catch(function (error) {
        onError(error);
      });
    return () => widget.remove();
  }, [onSuccess, onError]);

  return <div id={'osw-container'} />;
}
