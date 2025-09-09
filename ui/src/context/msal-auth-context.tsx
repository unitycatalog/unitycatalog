import React, { useCallback, useMemo } from 'react';
import {
  PublicClientApplication,
  Configuration,
  AccountInfo,
  AuthenticationResult,
  InteractionRequiredAuthError,
} from '@azure/msal-browser';
import { MsalProvider, useMsal, useAccount } from '@azure/msal-react';
import { useAuth } from './auth-context';

// MSAL Configuration using environment variables
const msalConfig: Configuration = {
  auth: {
    clientId: process.env.REACT_APP_AZURE_CLIENT_ID || 'test-client-id',
    authority: process.env.REACT_APP_AZURE_AUTHORITY || 'https://login.microsoftonline.com/common',
    redirectUri: window.location.origin + '/auth/callback',
  },
  cache: {
    cacheLocation: 'sessionStorage',
    storeAuthStateInCookie: false,
  },
};

// Initialize MSAL instance
const msalInstance = new PublicClientApplication(msalConfig);

// Login request with PKCE
const loginRequest = {
  scopes: ['openid', 'profile', 'email'],
  extraQueryParameters: {
    response_mode: 'fragment',
  },
};

interface MsalAuthContextProps {
  login: () => Promise<void>;
  logout: () => Promise<void>;
  getIdToken: () => Promise<string | null>;
  account: AccountInfo | null;
  isAuthenticated: boolean;
}

const MsalAuthContext = React.createContext<MsalAuthContextProps>({
  login: async () => {},
  logout: async () => {},
  getIdToken: async () => null,
  account: null,
  isAuthenticated: false,
});

function MsalAuthProvider({ children }: { children: React.ReactNode }) {
  const { instance, accounts } = useMsal();
  const account = useAccount(accounts[0] || {});
  const { loginWithToken, logout: backendLogout } = useAuth();

  const login = useCallback(async () => {
    try {
      const response: AuthenticationResult = await instance.loginPopup(loginRequest);
      if (response.idToken) {
        // Exchange Azure ID token for Unity Catalog token
        await loginWithToken(response.idToken);
      }
    } catch (error) {
      console.error('Login failed:', error);
      throw error;
    }
  }, [instance, loginWithToken]);

  const logout = useCallback(async () => {
    try {
      await backendLogout();
      await instance.logoutPopup();
    } catch (error) {
      console.error('Logout failed:', error);
      throw error;
    }
  }, [instance, backendLogout]);

  const getIdToken = useCallback(async () => {
    if (!account) return null;
    
    const request = {
      scopes: ['openid'],
      account: account,
    };

    try {
      const response = await instance.acquireTokenSilent(request);
      return response.idToken;
    } catch (error) {
      if (error instanceof InteractionRequiredAuthError) {
        const response = await instance.acquireTokenPopup(request);
        return response.idToken;
      }
      return null;
    }
  }, [instance, account]);

  const value = useMemo(() => ({
    login,
    logout,
    getIdToken,
    account,
    isAuthenticated: !!account,
  }), [login, logout, getIdToken, account]);

  return (
    <MsalAuthContext.Provider value={value}>
      {children}
    </MsalAuthContext.Provider>
  );
}

function useMsalAuth() {
  const context = React.useContext(MsalAuthContext);
  if (context === undefined) {
    throw new Error('useMsalAuth must be used within a MsalAuthProvider');
  }
  return context;
}

export { msalInstance, MsalAuthProvider, useMsalAuth };
