import { Flex, Layout, Typography } from 'antd';
import React from 'react';
import GoogleAuthButton from '../components/login/GoogleAuthButton';
import MSAuthButton from '../components/login/MSAuthButton';
import OktaAuthButton from '../components/login/OktaAuthButton';
import { useAuth } from '../context/auth-context';
import KeycloakAuthButton from '../components/login/KeycloakAuthButton';
import { useNavigate, useLocation } from 'react-router-dom';

import { MsalProvider } from "@azure/msal-react";
import { PublicClientApplication } from "@azure/msal-browser";

const msalConfig = {
    auth: {
        clientId: process.env.REACT_APP_MS_CLIENT_ID || "",
        authority: process.env.REACT_APP_MS_AUTHORITY || "",
        redirectUri: "http://localhost:3000",
    },
    cache: {
        cacheLocation: "localStorage",
        storeAuthStateInCookie: false,
    },
};
const msEnabled = process.env.REACT_APP_MS_AUTH_ENABLED === 'true';
if (!msalConfig.auth.clientId && msEnabled) {
    throw new Error("MSAL clientId is not defined. Please check your configuration.");
}
const msalInstance = new PublicClientApplication(msalConfig);

export default function LoginPage() {
    const {loginWithToken} = useAuth();
    const navigate = useNavigate();
    const location = useLocation();
    const from = location.state?.from || '/';
    const googleEnabled = process.env.REACT_APP_GOOGLE_AUTH_ENABLED === 'true';
    const oktaEnabled = process.env.REACT_APP_OKTA_AUTH_ENABLED === 'true';
    const keycloakEnabled =
        process.env.REACT_APP_KEYCLOAK_AUTH_ENABLED === 'true';

  const handleGoogleSignIn = async (idToken: string) => {
    await loginWithToken(idToken).then(() => navigate(from, { replace: true }));
  };

  const handleMSSignIn = async (idToken: string) => {
        await loginWithToken(idToken).then(() => navigate(from, { replace: true }));
    };

  return (
    <MsalProvider instance={msalInstance}>
      <Layout
        hasSider={false}
        style={{
          height: '100vh',
          width: '100vw',
          background: 'linear-gradient(#131D35,#252342,#1E2E3A)',
        }}
      >
        <Flex
          vertical={true}
          align={'center'}
          justify={'center'}
          gap={'middle'}
          style={{ height: '100%', width: '100%' }}
        >
          <div>
            <img
              src="/uc-logo-horiz-reverse.svg"
              height={32}
              alt="uc-logo-horizontal"
            />
          </div>
          <div
            style={{
              height: 276,
              width: 400,
              backgroundColor: '#F6F7F9',
              borderRadius: '16px',
            }}
          >
            <Flex
              vertical={true}
              align={'center'}
              justify={'center'}
              gap={'middle'}
              style={{ padding: 24 }}
            >
              <Typography.Title level={4}>
                Login to Unity Catalog
              </Typography.Title>
              {googleEnabled && (
                <GoogleAuthButton onGoogleSignIn={handleGoogleSignIn} />
              )}
              {msEnabled &&
                  (<MSAuthButton onMSSignIn={handleMSSignIn} />
              )}
              {oktaEnabled && (
                <OktaAuthButton
                  onSuccess={(tokens: any) => console.log('tokens', tokens)}
                  onError={(error: Error) => console.log('error', error)}
                />
              )}
              {keycloakEnabled && <KeycloakAuthButton />}
              {!googleEnabled && !oktaEnabled && !keycloakEnabled  && !msEnabled && (
                <Typography>Auth providers have not been enabled</Typography>
              )}
            </Flex>
          </div>
        </Flex>
      </Layout>
    </MsalProvider>
  );
}