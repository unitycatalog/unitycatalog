import { Flex, Layout, Typography, Button } from 'antd';
import React from 'react';
import GoogleAuthButton from '../components/login/GoogleAuthButton';
import MSAuthButton from '../components/login/MSAuthButton';
import OktaAuthButton from '../components/login/OktaAuthButton';
import { useAuth } from '../context/auth-context';
import KeycloakAuthButton from '../components/login/KeycloakAuthButton';
import { useNavigate, useLocation } from 'react-router-dom';
import { useMsalAuth } from '../context/msal-auth-context';

export default function LoginPage() {
    const {loginWithToken} = useAuth();
    const { login } = useMsalAuth();
    const navigate = useNavigate();
    const location = useLocation();
    const from = location.state?.from || '/';
    const googleEnabled = process.env.REACT_APP_GOOGLE_AUTH_ENABLED === 'true';
    const oktaEnabled = process.env.REACT_APP_OKTA_AUTH_ENABLED === 'true';
    const msEnabled = process.env.REACT_APP_MS_AUTH_ENABLED === 'true';
    const keycloakEnabled =
        process.env.REACT_APP_KEYCLOAK_AUTH_ENABLED === 'true';

  const handleGoogleSignIn = async (idToken: string) => {
    await loginWithToken(idToken).then(() => navigate(from, { replace: true }));
  };

  const handleMSSignIn = async (idToken: string) => {
        await loginWithToken(idToken).then(() => navigate(from, { replace: true }));
    };

  const handleAzureLogin = async () => {
    try {
      await login();
      navigate(from, { replace: true });
    } catch (error) {
      console.error('Azure login failed:', error);
    }
  };

  return (
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
              {msEnabled && (
                <>
                  <MSAuthButton onMSSignIn={handleMSSignIn} />
                  <Button 
                    type="primary" 
                    size="large" 
                    onClick={handleAzureLogin}
                    style={{ width: '100%' }}
                  >
                    Sign in with Azure AD (MSAL)
                  </Button>
                </>
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
  );
}