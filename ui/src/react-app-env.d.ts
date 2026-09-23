/// <reference types="react-scripts" />

// react-scripts only declares the `*.module.*` variants, so plain stylesheet
// side-effect imports have no declaration to resolve against.
declare module '*.css';
declare module '*.scss';
declare module '*.sass';
