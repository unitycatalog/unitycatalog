export const msalConfig = {
    auth: {
        clientId: "4f566254-43c5-4d47-9c08-11f6fae18046",
        authority: "https://login.microsoftonline.com/906aefe9-76a7-4f65-b82d-5ec20775d5aa",
        redirectUri: "http://localhost:3000",
    },
    cache: {
        cacheLocation: "localStorage",
        storeAuthStateInCookie: false,
    },
};

export const loginRequest = {
    scopes: ["openid", "profile", "email"]
};
