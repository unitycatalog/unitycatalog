import axios from 'axios';

const baseURL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8080';

export const bootstrapApi = {
  getBootstrapStatus: () => 
    axios.get(`${baseURL}/api/2.1/unity-catalog/admins/bootstrap-status`),
  
  claimAdmin: () => 
    axios.post(`${baseURL}/api/2.1/unity-catalog/admins/claim-admin`),
};
