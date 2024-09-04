import axios from 'axios';
import { UC_API_PREFIX } from '../utils/constants';

const client = axios.create({
  baseURL: `${UC_API_PREFIX}`,
  headers: {
    'Content-type': 'application/json',
  },
});

export default client;
