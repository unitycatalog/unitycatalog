import React from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import { useMsalAuth } from '../context/msal-auth-context';
import { useAuth } from '../context/auth-context';
import { Spin } from 'antd';

interface RequireAuthProps {
  children: React.ReactNode;
}

export function RequireAuth({ children }: RequireAuthProps) {
  const { isAuthenticated } = useMsalAuth();
  const { currentUser } = useAuth();
  const location = useLocation();

  // Show loading while checking authentication status
  if (isAuthenticated && currentUser === undefined) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh' }}>
        <Spin size="large" />
      </div>
    );
  }

  // Redirect to login if not authenticated
  if (!isAuthenticated || !currentUser) {
    return <Navigate to="/login" state={{ from: location }} replace />;
  }

  // Render protected content
  return <>{children}</>;
}
