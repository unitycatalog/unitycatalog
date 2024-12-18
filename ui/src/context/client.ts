import axios from 'axios';
import { UC_API_PREFIX } from '../utils/constants';

/**
 * The singleton client instance with `baseURL`.
 */
export const CLIENT = axios.create({
  baseURL: `${UC_API_PREFIX}`,
  headers: {
    'Content-type': 'application/json',
  },
});
