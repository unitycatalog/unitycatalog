import { TTL_OPTIONS, MAX_TTL_SECONDS } from '../tokens';

describe('Token Types - Standalone', () => {
  test('TTL options are properly defined', () => {
    expect(TTL_OPTIONS).toHaveLength(5);
    expect(TTL_OPTIONS[0].label).toBe('1 hour');
    expect(TTL_OPTIONS[0].seconds).toBe(3600);
    expect(TTL_OPTIONS[4].label).toBe('60 days (max)');
    expect(TTL_OPTIONS[4].seconds).toBe(5184000);
  });

  test('MAX_TTL_SECONDS is correct', () => {
    expect(MAX_TTL_SECONDS).toBe(5184000); // 60 days in seconds
  });
});
