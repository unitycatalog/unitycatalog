import { Avatar, Button } from 'antd';
// getting errors with node version in jwt-decode dependency, removed package for now
// import Keycloak from 'keycloak-js';
// import { useState } from 'react';

export default function KeycloakAuthButton() {
  // const [keycloak, setKeycloak] = useState<Keycloak | null>(null);

  // const initKeycloak = () => {
  //   const kc = new Keycloak({
  //     url: process.env.REACT_APP_KEYCLOAK_URL || '',
  //     realm: process.env.REACT_APP_KEYCLOAK_REALM_ID || '',
  //     clientId: process.env.REACT_APP_KEYCLOAK_CLIENT_ID || '',
  //   });
  //
  //   kc.init({ onLoad: 'check-sso' })
  //     .then((auth) => {
  //       setKeycloak(kc);
  //     })
  //     .catch((error) => {
  //       console.error('Failed to initialize Keycloak:', error);
  //     });
  // };

  // const handleLogin = () => {
  //   if (keycloak) {
  //     keycloak.login();
  //   } else {
  //     initKeycloak();
  //   }
  // };

  return (
    <Button
      icon={
        <Avatar
          shape={'square'}
          src={'/keycloak-logo.svg'}
          style={{ width: 20, height: 20, marginRight: 16 }}
        />
      }
      iconPosition={'start'}
      style={{ width: 240, height: 40, justifyContent: 'flex-start' }}
      // onClick={() => handleLogin()}
    >
      Continue with Keycloak
    </Button>
  );
}
