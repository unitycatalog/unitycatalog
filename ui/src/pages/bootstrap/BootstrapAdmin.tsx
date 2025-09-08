import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Button, Alert, Box, Typography, Paper, CircularProgress } from '@mui/material';
import { bootstrapApi } from '../../utils/api';

interface BootstrapStatus {
  bootstrapEnabled: boolean;
  hasAzureAdmin: boolean;
  allowedDomains?: string[];
}

export const BootstrapAdmin: React.FC = () => {
  const navigate = useNavigate();
  const [status, setStatus] = useState<BootstrapStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  useEffect(() => {
    fetchBootstrapStatus();
  }, []);

  const fetchBootstrapStatus = async () => {
    try {
      setLoading(true);
      const response = await bootstrapApi.getBootstrapStatus();
      setStatus(response.data);
    } catch (err: any) {
      setError(err.response?.data?.message || 'Failed to fetch bootstrap status');
    } finally {
      setLoading(false);
    }
  };

  const handleClaimAdmin = async () => {
    try {
      setLoading(true);
      setError(null);
      await bootstrapApi.claimAdmin();
      setSuccess(true);
    } catch (err: any) {
      setError(err.response?.data?.message || 'Failed to claim admin privileges');
    } finally {
      setLoading(false);
    }
  };

  if (loading && !status) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
        <CircularProgress />
      </Box>
    );
  }

  if (success) {
    return (
      <Box sx={{ p: 4, textAlign: 'center', maxWidth: 600, mx: 'auto' }}>
        <Alert severity="success" sx={{ mb: 3 }}>
          Admin privileges claimed successfully!
        </Alert>
        <Typography variant="body1" sx={{ mb: 3 }}>
          Use the following command to continue:
        </Typography>
        <Paper sx={{ p: 2, bgcolor: 'grey.100', fontFamily: 'monospace' }}>
          bin/uc auth login --output jsonPretty
        </Paper>
        <Button 
          variant="contained" 
          sx={{ mt: 3 }}
          onClick={() => navigate('/')}
        >
          Continue to Unity Catalog
        </Button>
      </Box>
    );
  }

  if (!status?.bootstrapEnabled) {
    return (
      <Box sx={{ p: 4, maxWidth: 600, mx: 'auto' }}>
        <Alert severity="warning">
          Bootstrap functionality is not enabled on this server.
        </Alert>
      </Box>
    );
  }

  if (status.hasAzureAdmin) {
    return (
      <Box sx={{ p: 4, maxWidth: 600, mx: 'auto' }}>
        <Alert severity="info">
          An Azure admin has already been configured for this server.
        </Alert>
      </Box>
    );
  }

  return (
    <Box sx={{ p: 4, maxWidth: 600, mx: 'auto' }}>
      <Typography variant="h4" gutterBottom>Claim Admin Privileges</Typography>
      <Typography variant="body1" sx={{ mb: 3 }}>
        This Unity Catalog instance needs an initial administrator. You can claim admin privileges using your Azure AD credentials.
      </Typography>
      
      {status.allowedDomains && status.allowedDomains.length > 0 && (
        <Alert severity="info" sx={{ mb: 2 }}>
          Only users from the following domains can claim admin privileges: {status.allowedDomains.join(', ')}
        </Alert>
      )}
      
      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
      
      <Button 
        variant="contained" 
        size="large" 
        onClick={handleClaimAdmin}
        disabled={loading}
        fullWidth
      >
        {loading ? 'Claiming...' : 'Claim Admin Privileges'}
      </Button>
    </Box>
  );
};
