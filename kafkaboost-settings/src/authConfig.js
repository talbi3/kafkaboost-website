const cognitoAuthConfig = {
    authority: "https://eu-north-1v6hez02lg.auth.eu-north-1.amazoncognito.com",
    client_id: "3m8nr1udadq2kqnkj27l9b2bi1",
    redirect_uri: "http://tal-noa-reactapp-kafkaboost.s3-website.eu-north-1.amazonaws.com/",
    response_type: "code",
    scope: "openid email profile",
  };
  
  export default cognitoAuthConfig;
  