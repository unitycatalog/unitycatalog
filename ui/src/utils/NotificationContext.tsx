import React, { createContext, useContext, useState, ReactNode } from 'react';
import { notification } from 'antd';

interface NotificationContextType {
  setNotification: (
    message: string,
    type: 'success' | 'error' | 'info' | 'warning',
  ) => void;
}

const NotificationContext = createContext<NotificationContextType | undefined>(
  undefined,
);

export const NotificationProvider: React.FC<{ children: ReactNode }> = ({
  children,
}) => {
  const [notificationData, setNotificationData] = useState<{
    message: string;
    type: 'success' | 'error' | 'info' | 'warning';
  } | null>(null);

  const setNotification = (
    message: string,
    type: 'success' | 'error' | 'info' | 'warning',
  ) => {
    setNotificationData({ message, type });
  };

  React.useEffect(() => {
    if (notificationData) {
      notification[notificationData.type]({
        message: notificationData.message,
      });
      setNotificationData(null);
    }
  }, [notificationData]);

  return (
    <NotificationContext.Provider value={{ setNotification }}>
      {children}
    </NotificationContext.Provider>
  );
};

export const useNotification = (): NotificationContextType => {
  const context = useContext(NotificationContext);
  if (!context) {
    throw new Error(
      'useNotification must be used within a NotificationProvider',
    );
  }
  return context;
};
