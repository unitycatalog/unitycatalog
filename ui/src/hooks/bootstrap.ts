import { useQuery } from '@tanstack/react-query';

interface BootstrapStatus {
  available: boolean;
  needsBootstrap: boolean;
  error?: string;
}

export const useBootstrapStatus = () => {
  return useQuery<BootstrapStatus>({
    queryKey: ['bootstrap-status'],
    queryFn: async (): Promise<BootstrapStatus> => {
      try {
        // Test if bootstrap endpoint is available
        const response = await fetch('/api/1.0/unity-control/auth/azure-login/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: '{}'
        });
        
        if (response.ok) {
          return { available: true, needsBootstrap: true };
        } else if (response.status === 409) {
          // Conflict - admin already exists
          return { available: false, needsBootstrap: false };
        } else {
          return { available: false, needsBootstrap: false, error: 'Bootstrap not enabled' };
        }
      } catch (error) {
        return { available: false, needsBootstrap: false, error: 'Bootstrap not available' };
      }
    },
    retry: false,
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
};
