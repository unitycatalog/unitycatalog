import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useGetCurrentUser, useLoginWithToken } from '../hooks/user';
import catalogClient from './catalog';
import { useNotification } from '../utils/NotificationContext';

interface AuthContextProps {
  accessToken: any;
  loginWithToken: any;
  logout: any;
  currentUser: any;
}

const AuthContext = React.createContext<AuthContextProps>({
  accessToken: null,
  loginWithToken: null,
  logout: null,
  currentUser: null,
});
AuthContext.displayName = 'AuthContext';

function AuthProvider(props: any) {
  const [accessToken, setAccessToken] = useState<string>('');
  const { data: currentUser } = useGetCurrentUser(accessToken);
  const loginWithTokenMutation = useLoginWithToken();
  const { setNotification } = useNotification();

  const loginWithToken = useCallback(
    async (idToken: string) => {
      return loginWithTokenMutation.mutate(idToken, {
        onSuccess: (response) => {
          setAccessToken(response.access_token);
        },
        onError: (error) => {
          setNotification(
            'Login failed. Please contact your system administrator.',
            'error',
          );
        },
      });
    },
    [loginWithTokenMutation, setNotification],
  );

  const logout = useCallback(async () => {
    return setAccessToken('');
  }, []);

  useEffect(() => {
    const requestIntercept = catalogClient.interceptors.request.use(
      (config) => {
        if (accessToken) {
          config.headers['Authorization'] = `Bearer ${accessToken}`;
        }
        return config;
      },
      (error) => Promise.reject(error),
    );
    const responseIntercept = catalogClient.interceptors.response.use(
      (response) => response,
      async (error) => {
        if (error?.response?.status === 403) {
          // todo if we support refresh in the future, that logic will go in here
          setAccessToken('');
        }
        return Promise.reject(error);
      },
    );

    return () => {
      catalogClient.interceptors.request.eject(requestIntercept);
      catalogClient.interceptors.response.eject(responseIntercept);
    };
  }, [accessToken]);

  const value = useMemo(
    () => ({
      accessToken,
      loginWithToken,
      logout,
      currentUser,
    }),
    [accessToken, loginWithToken, logout, currentUser],
  );

  return <AuthContext.Provider value={value} {...props} />;
}

function useAuth() {
  const context = React.useContext(AuthContext);
  if (context === undefined) {
    throw new Error(`useAuth must be used within an AuthProvider`);
  }
  return context;
}

export { AuthProvider, useAuth };
