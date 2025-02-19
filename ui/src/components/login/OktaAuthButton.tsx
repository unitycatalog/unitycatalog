import { Avatar, Button, Modal } from 'antd';
import { useState } from 'react';
import OktaSignInWidget from './OktaSignInWidget';

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
        destroyOnClose={true}
        onCancel={() => {
          setWidgetModalOpen(false);
        }}
      >
        <OktaSignInWidget onSuccess={onSuccess} onError={onError} />
      </Modal>
    </>
  );
}
