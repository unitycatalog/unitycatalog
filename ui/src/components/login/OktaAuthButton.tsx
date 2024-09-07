import { Avatar, Button, Modal } from 'antd';
import { useEffect, useRef, useState } from 'react';
import OktaSignIn from '@okta/okta-signin-widget';

interface OktaAuthButtonProps {
  onSuccess: (tokens: any) => void;
  onError: (error: Error) => void;
}

export default function OktaAuthButton({
  onSuccess,
  onError,
}: OktaAuthButtonProps) {
  const widgetRef = useRef<HTMLDivElement | null>(null);
  const [widgetModalOpen, setWidgetModalOpen] = useState(false);

  useEffect(() => {
    if (!widgetModalOpen) return;

    const widget = new OktaSignIn({
      baseUrl: process.env.REACT_APP_OKTA_DOMAIN,
      clientId: process.env.REACT_APP_OKTA_CLIENT_ID,
      redirectUri: window.location.origin + '/login/callback',
      authParams: {
        issuer: process.env.REACT_APP_OKTA_DOMAIN + '/oauth2/default',
        scopes: ['openid', 'profile', 'email'],
      },
    });

    widget.renderEl(
      //@ts-ignore
      { el: widgetRef.current },
      onSuccess,
      onError,
    );

    return () => widget.remove();
  }, [widgetModalOpen, onSuccess, onError]);

  return (
    <>
      <Button
        icon={
          <Avatar
            src={'/okta-logo.png'}
            style={{ width: 20, height: 20, marginRight: 16 }}
          />
        }
        iconPosition={'start'}
        style={{ width: 240, height: 40, justifyContent: 'flex-start' }}
        onClick={() => setWidgetModalOpen(true)}
      >
        Continue with Okta
      </Button>
      <Modal open={widgetModalOpen}>
        <div ref={widgetRef} />
      </Modal>
    </>
  );
}
