// jest-dom adds custom jest matchers for asserting on DOM nodes.
// allows you to do things like:
// expect(element).toHaveTextContent(/react/i)
// learn more: https://github.com/testing-library/jest-dom
import '@testing-library/jest-dom';
import { TextEncoder, TextDecoder } from 'util';

// Polyfill crypto for Jest environment
Object.assign(global, { TextDecoder, TextEncoder });

// Mock crypto for MSAL
Object.defineProperty(global, 'crypto', {
  value: {
    getRandomValues: jest.fn(() => new Uint32Array(10)),
    subtle: {
      digest: jest.fn(() => Promise.resolve(new ArrayBuffer(32))),
    },
  },
});

// Mock window.location
Object.defineProperty(window, 'location', {
  value: {
    origin: 'http://localhost:3000',
    pathname: '/login',
    assign: jest.fn(),
    replace: jest.fn(),
  },
  writable: true,
});
