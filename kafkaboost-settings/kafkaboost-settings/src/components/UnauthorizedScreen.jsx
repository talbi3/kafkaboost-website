import React from 'react';
import './UnauthorizedScreen.css';
import logo from '../assets/kafkaboost-logo.png';

function UnauthorizedScreen({ onLogin }) {
  return (
    <div className="unauthorized-screen">
      <div className="login-card">
        <img src={logo} alt="KafkaBoost Logo" className="login-logo" />
        <h2 className="login-title">Sign in to KafkaBoost</h2>
        <button className="login-button" onClick={onLogin}>Sign In</button>
      </div>
    </div>
  );
}

export default UnauthorizedScreen;
