import axios from 'axios';
import { UC_AUTH_API_PREFIX } from '../utils/constants';

/**
 * The singleton client instance with `baseURL`.
 */
export const CLIENT = axios.create({
  baseURL: `${UC_AUTH_API_PREFIX}`,
  headers: {
    'Content-type': 'application/json',
  },
});
