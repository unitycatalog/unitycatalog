import React, { useCallback, useMemo } from 'react';
import {
  useGetCurrentUser,
  useLoginWithToken,
  useLogoutCurrentUser,
  UserInterface,
} from '../hooks/user';
import { GrantType, TokenType } from '../types/api/control.gen';
import { useNotification } from '../utils/NotificationContext';

interface AuthContextProps {
  accessToken: any;
  loginWithToken: any;
  logout: any;
  currentUser: UserInterface | null;
}

const AuthContext = React.createContext<AuthContextProps>({
  accessToken: null,
  loginWithToken: null,
  logout: null,
  currentUser: null,
});
AuthContext.displayName = 'AuthContext';

function AuthProvider(props: any) {
  const { data: currentUser, refetch } = useGetCurrentUser();
  const loginWithTokenMutation = useLoginWithToken();
  const logoutUser = useLogoutCurrentUser();
  const { setNotification } = useNotification();

  const loginWithToken = useCallback(
    async (idToken: string) => {
      return loginWithTokenMutation.mutate(
        {
          grant_type: GrantType.TOKEN_EXCHANGE,
          requested_token_type: TokenType.ACCESS_TOKEN,
          subject_token_type: TokenType.ID_TOKEN,
          subject_token: idToken,
        },
        {
          onSuccess: () => {
            refetch();
          },
          onError: () => {
            setNotification(
              'Login failed. Please contact your system administrator.',
              'error',
            );
          },
        },
      );
    },
    [loginWithTokenMutation, setNotification, refetch],
  );

  const logout = useCallback(async () => {
    return logoutUser.mutate(
      {},
      {
        onSuccess: () => {
          refetch();
        },
        onError: () => {
          setNotification(
            'Logout failed. Please contact your system administrator.',
            'error',
          );
        },
      },
    );
  }, [refetch, logoutUser, setNotification]);

  const value = useMemo(
    () => ({
      loginWithToken,
      logout,
      currentUser,
    }),
    [loginWithToken, logout, currentUser],
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
