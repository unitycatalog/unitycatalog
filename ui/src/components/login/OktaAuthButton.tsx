import { Avatar, Button, Modal } from 'antd';
import { useState } from 'react';
import OktaSignIn from '@okta/okta-signin-widget';

interface OktaAuthButtonProps {
  onSuccess: (tokens: any) => void;
  onError: (error: Error) => void;
}

export default function OktaAuthButton({
  onSuccess,
  onError,
}: OktaAuthButtonProps) {
  const [widgetModalOpen, setWidgetModalOpen] = useState(false);

  const handleOnClick = () => {
    setWidgetModalOpen(true);

    setTimeout(() => {
      const widget = new OktaSignIn({
        clientId: process.env.REACT_APP_OKTA_CLIENT_ID,
        redirectUri: window.location.origin,
        issuer:
          'https://' + process.env.REACT_APP_OKTA_DOMAIN + '/oauth2/default',
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
    }, 1000);
  };

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
        onClick={() => handleOnClick()}
      >
        Continue with Okta
      </Button>
      <Modal
        open={widgetModalOpen}
        footer={null}
        onCancel={() => setWidgetModalOpen(false)}
        destroyOnClose={true}
      >
        <div id={'osw-container'} />
      </Modal>
    </>
  );
}
