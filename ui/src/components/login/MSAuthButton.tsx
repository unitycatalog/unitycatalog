import React from "react";
import { useMsal } from "@azure/msal-react";
import { Avatar, Button } from 'antd';

const loginRequest = {
  scopes: ["openid", "profile", "email"]
};
interface MSAuthButtonProps {
  onMSSignIn: (idToken: string) => Promise<void>;
}

const MSAuthButton: React.FC<MSAuthButtonProps> = ({ onMSSignIn }) => {
  const { instance } = useMsal();

  const handleLogin = async () => {
    try {
      const response = await instance.loginPopup(loginRequest);
      const idToken = response.idToken; // Extract the idToken from the response
      await onMSSignIn(idToken);
    } catch (error) {
      console.error("Login failed:", error);
    }
  };

  return (
    <Button
      onClick={handleLogin}
      icon={
        <Avatar
            src={'/ms.png'}
            style={{ width: 25, height: 25, marginRight: 20 }}
        />
      }
    >
      Login with Microsoft
    </Button>
  );
};

export default MSAuthButton;